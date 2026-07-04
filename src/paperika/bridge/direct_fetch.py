"""Tier 1 of the paperika download ladder (AGA-339): a deterministic direct-HTTP
PDF fetch that runs BEFORE the tier-3 codex executor.

The gpu host has IP-based institutional access, so for the common case a plain
authenticated HTTP GET of the article-level PDF succeeds without driving a browser
at all — no ~117s codex session required. This module resolves the DOI (following
the doi.org redirect chain), finds the PDF URL with a small deterministic ruleset
(the ``citation_pdf_url`` meta tag plus a per-publisher table — the same heuristics
the codex prompt in executor.py encodes, ported to code), fetches the bytes through
the SAME authenticated session (Chrome cookies exported over CDP), validates the
``%PDF-`` magic + a 10 KB floor, and lands the file in the configured
``download_dir`` as ``<sanitized-doi>.pdf`` so app.py's existing containment /
mtime-window / identity-verify gates keep holding.

Security invariants (mirroring the codex-executor containment, §2.4):
- Every fetched URL and every byte of returned HTML is treated strictly as DATA;
  none of it is ever executed or interpolated into a prompt.
- Redirects are followed MANUALLY (httpx ``follow_redirects`` is off) and every
  hop — plus the start URL and every candidate PDF URL — is gated through
  ``is_public_http_url``: http(s) scheme only, and the host must resolve
  exclusively to public/global IPs. A hop pointed at an internal target
  (loopback / link-local / private / reserved — e.g. the CDP control port
  127.0.0.1:9224, cloud metadata 169.254.169.254, the bridge's own listener)
  aborts the fetch instead of being followed (SSRF containment).
- The bearer token is never referenced or logged here.
- Unverified bytes are NEVER returned to the caller. This module only WRITES a
  candidate file into ``download_dir`` (guarded by the %PDF- magic + 10 KB floor);
  app.py's verify path owns identity verification and streaming it back.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import html
import ipaddress
from pathlib import Path
import re
import socket
from urllib.parse import parse_qs, urljoin, urlparse

import httpx

# A current desktop-Chrome UA (the gpu host is Linux; matches the managed
# Chrome/147 the bridge drives). Publisher access is by IP, but a real-browser UA
# avoids trivial UA-based bot gates.
_CHROME_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
)
_ACCEPT = "application/pdf,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"

# Read caps: HTML landing pages are parsed for a PDF URL (2 MB is plenty); a body
# whose first bytes are %PDF- is a download and gets the generous PDF cap. app.py's
# MAX_FILE_SIZE (500 MB) is the final ceiling; this cap just bounds memory here.
_MAX_HTML_BYTES = 2 * 1024 * 1024
_MAX_PDF_BYTES = 100 * 1024 * 1024
_MIN_PDF_BYTES = 10_000

# Cloudflare / challenge-page markers (lower-cased substring match on the body).
_CHALLENGE_MARKERS = ("cf-chl", "just a moment", "cf-browser-verification", "attention required")

# Filename sanitizer, shared with the tier-2 fetcher so both tiers land a file
# under the identical name (idempotent re-fetch upserts in place). Kept consistent
# with the ``<sanitized-doi>.pdf`` the codex prompt writes.
_DOI_SANITIZE_RE = re.compile(r"[^A-Za-z0-9._-]")

# Tolerant citation_pdf_url extraction — the two <meta> attributes appear in either
# order across publishers, values may be single- or double-quoted and HTML-escaped.
_META_NAME_FIRST = re.compile(
    r"""<meta\b[^>]*?\bname\s*=\s*["']citation_pdf_url["'][^>]*?"""
    r"""\bcontent\s*=\s*["']([^"']*)["']""",
    re.IGNORECASE | re.DOTALL,
)
_META_CONTENT_FIRST = re.compile(
    r"""<meta\b[^>]*?\bcontent\s*=\s*["']([^"']*)["'][^>]*?"""
    r"""\bname\s*=\s*["']citation_pdf_url["']""",
    re.IGNORECASE | re.DOTALL,
)

# A candidate URL can serve a short HTML "delivery" interstitial instead of the
# PDF (Optica's viewmedia.cfm → view_article.cfm "your PDF will open shortly",
# whose only real link is the /directpdfaccess/…/file.pdf URL). Pull an
# href/src that points at a directpdfaccess path or ends in .pdf so tier 1 can
# follow ONE interstitial hop instead of falling through to the browser tiers.
_INTERSTITIAL_PDF_HREF = re.compile(
    r"""(?:href|src)\s*=\s*["']([^"']*(?:/directpdfaccess/[^"']+|\.pdf(?:\?[^"']*)?))["']""",
    re.IGNORECASE,
)

# Test seam: when set (by tests, via monkeypatch), used as the httpx transport so
# the ladder never touches the real network. None in production ⇒ real transport.
_TEST_TRANSPORT: httpx.AsyncBaseTransport | None = None


# --- SSRF guard (AGA-339 review fix) --------------------------------------
# Untrusted publisher HTML (the citation_pdf_url meta, anchor hrefs) and the
# redirect chain are DATA: none of it may steer an authenticated GET at an
# internal host — the CDP control port (127.0.0.1:9224), cloud metadata
# (169.254.169.254), the bridge's own listener, or any private/reserved service.
# Every URL fetched (start_url, each redirect hop, every candidate) is gated by
# ``is_public_http_url`` before the request goes out. Shared with tier 2
# (scripted_browser.pick_pdf_anchor) so both deterministic tiers refuse the same
# targets and stay consistent.

# Hostnames refused without a DNS lookup (defense in depth; each also resolves to
# an internal address that the resolver check would catch anyway).
_BLOCKED_HOSTNAMES = frozenset({"localhost"})

# Cap the manual redirect chain so a redirect loop can't spin.
_MAX_REDIRECTS = 10

# Optica (opg.optica.org) generates the article PDF on demand: the first hit on
# viewmedia.cfm returns 202 Accepted with a "generating" HTML body, then serves
# the real %PDF- on a retry a few seconds later. Retry a 202 candidate a bounded
# number of times (budget-checked) before giving up on it.
_GENERATING_STATUSES = frozenset({202})
_GENERATING_RETRIES = 3
_GENERATING_BACKOFF_SECONDS = 2.0


class _BlockedTarget(httpx.HTTPError):
    """A URL / redirect hop pointed at a non-public host — refused. Subclasses
    ``httpx.HTTPError`` so the existing fetch error handling classifies it as an
    ordinary transport failure (the ladder never crashes on it)."""


def _addr_is_public(ip: str) -> bool:
    """True only for a globally-routable unicast address."""
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False
    return not (
        addr.is_loopback
        or addr.is_link_local
        or addr.is_private
        or addr.is_reserved
        or addr.is_multicast
        or addr.is_unspecified
    )


def _resolve_host(host: str) -> list[str]:
    """Resolve ``host`` to its IP strings. A module-level seam so tests can inject
    a deterministic map instead of touching real DNS."""
    infos = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
    return [info[4][0] for info in infos]


def is_public_http_url(url: str) -> bool:
    """True only for an http(s) URL whose host is a public address (an IP literal
    is checked directly; a hostname must resolve EXCLUSIVELY to public addresses,
    so a name that resolves to any loopback/link-local/private/reserved IP — incl.
    DNS-rebinding to an internal target — is refused)."""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return False
    host = (parsed.hostname or "").strip().lower()
    if not host or host in _BLOCKED_HOSTNAMES:
        return False
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass  # not an IP literal — fall through to DNS resolution
    else:
        return _addr_is_public(host)
    try:
        addrs = _resolve_host(host)
    except OSError:
        return False
    return bool(addrs) and all(_addr_is_public(a) for a in addrs)


@dataclass(slots=True)
class DirectFetchResult:
    """Outcome of a tier-1 direct fetch. ``kind`` is one of
    ``downloaded`` / ``no_pdf`` / ``wall`` / ``error``. On ``downloaded``,
    ``file_path`` points at the PDF written into ``download_dir``."""

    kind: str
    file_path: str | None = None
    final_url: str | None = None
    notes: str = ""
    tried: list[str] = field(default_factory=list)


def sanitize_doi(doi: str) -> str:
    """Deterministic DOI→filename stem: any char outside [A-Za-z0-9._-] → '_'."""
    return _DOI_SANITIZE_RE.sub("_", doi)


async def export_chrome_cookies(cdp_http_url: str) -> list[dict]:
    """Export cookies from the EXISTING default context of the managed Chrome over
    CDP so the tier-1 HTTP fetch reuses the browser's authenticated session.

    Reads ``context.cookies()`` without opening any page and without closing the
    browser — only the playwright driver connection is torn down (by exiting the
    ``async_playwright`` context). Any failure degrades to a cookie-less fetch:
    tier 1 must never crash the ladder, so this always returns a list.
    """
    try:
        from playwright.async_api import async_playwright
    except Exception:
        return []
    try:
        async with async_playwright() as p:
            browser = await p.chromium.connect_over_cdp(cdp_http_url)
            contexts = browser.contexts
            if not contexts:
                return []
            cookies = await contexts[0].cookies()
            # Do NOT browser.close() — leave Chrome (and its pages) running; the
            # async_playwright context exit drops only the driver connection.
            return [dict(c) for c in cookies]
    except Exception:
        return []


def _build_cookie_jar(raw: list[dict]) -> httpx.Cookies:
    """Build a domain/path-aware httpx cookie jar from exported browser cookies."""
    jar = httpx.Cookies()
    for c in raw:
        try:
            name = c.get("name")
            value = c.get("value")
            if not name or value is None:
                continue
            jar.set(name, str(value), domain=c.get("domain") or "", path=c.get("path") or "/")
        except Exception:
            continue
    return jar


def _timeout(budget_seconds: float) -> httpx.Timeout:
    per_request = max(1.0, min(budget_seconds, 15.0))
    return httpx.Timeout(per_request, connect=min(per_request, 10.0))


def _build_async_client(*, cookies: httpx.Cookies, timeout: httpx.Timeout) -> httpx.AsyncClient:
    kwargs: dict = {
        # Redirects are followed MANUALLY in _get so the SSRF guard re-checks
        # every hop; NEVER let httpx auto-follow off to an unguarded host.
        "follow_redirects": False,
        "timeout": timeout,
        "headers": {"User-Agent": _CHROME_UA, "Accept": _ACCEPT},
        "cookies": cookies,
    }
    if _TEST_TRANSPORT is not None:
        kwargs["transport"] = _TEST_TRANSPORT
    return httpx.AsyncClient(**kwargs)


async def _read_capped(resp: httpx.Response) -> bytes:
    """Stream the body, capping at the HTML limit unless the first bytes are the
    %PDF- magic (then the PDF limit). Bounds memory on both HTML and PDF paths."""
    chunks: list[bytes] = []
    total = 0
    is_pdf = False
    decided = False
    cap = _MAX_HTML_BYTES
    async for chunk in resp.aiter_bytes():
        chunks.append(chunk)
        total += len(chunk)
        if not decided and total >= 5:
            is_pdf = b"".join(chunks)[:5].startswith(b"%PDF-")
            cap = _MAX_PDF_BYTES if is_pdf else _MAX_HTML_BYTES
            decided = True
        if total >= cap:
            break
    body = b"".join(chunks)
    return body[: (_MAX_PDF_BYTES if is_pdf else _MAX_HTML_BYTES)]


async def _get(client: httpx.AsyncClient, url: str) -> tuple[int, bytes, str]:
    """GET ``url`` following redirects MANUALLY so the SSRF guard re-checks every
    hop (``follow_redirects`` is off on the client). Return status, capped body,
    and the final URL. A hop to a non-public host raises ``_BlockedTarget`` (an
    ``httpx.HTTPError``) and aborts the fetch rather than being followed."""
    current = httpx.URL(url)
    for _ in range(_MAX_REDIRECTS + 1):
        if not is_public_http_url(str(current)):
            raise _BlockedTarget(f"refused non-public target: {current}")
        async with client.stream("GET", current) as resp:
            location = resp.headers.get("location") if resp.is_redirect else None
            if location:
                current = resp.url.join(location)
                continue
            body = await _read_capped(resp)
            return resp.status_code, body, str(resp.url)
    raise _BlockedTarget(f"too many redirects from {url}")


def _passes_pdf_gate(body: bytes) -> bool:
    return body.startswith(b"%PDF-") and len(body) > _MIN_PDF_BYTES


def _looks_like_wall(status: int, body: bytes) -> bool:
    if status == 403:
        return True
    prefix = body[:4096].decode("utf-8", "replace").lower()
    return any(marker in prefix for marker in _CHALLENGE_MARKERS)


def _citation_pdf_url(html_text: str, base_url: str) -> str | None:
    """Extract <meta name="citation_pdf_url" content="..."> (either attribute
    order), HTML-unescape it, and resolve it against the landing URL."""
    for rx in (_META_NAME_FIRST, _META_CONTENT_FIRST):
        m = rx.search(html_text)
        if m:
            value = html.unescape(m.group(1)).strip()
            if value:
                return urljoin(base_url, value)
    return None


def _interstitial_pdf_url(html_text: str, base_url: str) -> str | None:
    """Pull the real PDF URL out of an HTML delivery interstitial: the
    citation_pdf_url meta if present, else the first directpdfaccess/*.pdf href.
    Resolved against the interstitial's own URL. None if nothing PDF-ish is found."""
    meta = _citation_pdf_url(html_text, base_url)
    if meta:
        return meta
    m = _INTERSTITIAL_PDF_HREF.search(html_text)
    if m:
        return urljoin(base_url, html.unescape(m.group(1)).strip())
    return None


def _publisher_candidates(final_url: str, doi: str) -> list[str]:
    """Per-publisher PDF-URL rules keyed on the landing netloc. Easy to extend:
    add a branch that returns the article-level PDF URL(s) for a new host."""
    parsed = urlparse(final_url)
    netloc = parsed.netloc.lower()
    path = parsed.path
    out: list[str] = []

    if netloc == "ieeexplore.ieee.org":
        m = re.search(r"/document/(\d+)", path)
        if m:
            out.append(f"https://ieeexplore.ieee.org/stampPDF/getPDF.jsp?tp=&arnumber={m.group(1)}")
    elif netloc in {"opg.optica.org", "www.osapublishing.org"}:
        query = parse_qs(parsed.query)
        uri = None
        for key in ("uri", "URI"):
            if key in query and query[key]:
                uri = query[key][0]
                break
        if uri:
            out.append(f"https://{netloc}/viewmedia.cfm?uri={uri}&seq=0")
    elif netloc == "link.springer.com":
        out.append(f"https://link.springer.com/content/pdf/{doi}.pdf")
    elif netloc == "onlinelibrary.wiley.com":
        out.append(f"https://onlinelibrary.wiley.com/doi/pdf/{doi}?download=true")
    elif netloc == "www.mdpi.com":
        base = final_url.split("#", 1)[0].split("?", 1)[0].rstrip("/")
        out.append(f"{base}/pdf")
    elif netloc == "www.nature.com":
        m = re.search(r"/articles/([^/?#]+)", path)
        if m:
            out.append(f"https://www.nature.com/articles/{m.group(1)}.pdf")
    elif netloc == "arxiv.org":
        m = re.search(r"/abs/([^/?#]+)", path)
        if m:
            out.append(f"https://arxiv.org/pdf/{m.group(1)}")

    return out


def _candidate_urls(final_url: str, doi: str, html_body: bytes) -> list[str]:
    """Candidate PDF URLs in priority order: citation_pdf_url meta first, then the
    per-publisher rules. Deduped, order preserved."""
    out: list[str] = []

    def add(url: str | None) -> None:
        if url and url not in out:
            out.append(url)

    add(_citation_pdf_url(html_body.decode("utf-8", "replace"), final_url))
    for url in _publisher_candidates(final_url, doi):
        add(url)
    return out


def _note(prefix: str, tried: list[str]) -> str:
    trace = " | ".join(tried)
    return f"{prefix}: {trace}" if trace else prefix


def _land(
    download_dir: Path, doi: str, body: bytes, final_url: str, tried: list[str], *, note: str
) -> DirectFetchResult:
    download_dir.mkdir(parents=True, exist_ok=True)
    path = download_dir / f"{sanitize_doi(doi)}.pdf"
    path.write_bytes(body)
    tried.append(f"wrote {path}")
    return DirectFetchResult(
        kind="downloaded",
        file_path=str(path),
        final_url=final_url,
        notes=_note(f"downloaded ({note})", tried),
        tried=tried,
    )


async def _run(
    *, doi: str, start_url: str, download_dir: Path, jar: httpx.Cookies, budget_seconds: float,
    tried: list[str],
) -> DirectFetchResult:
    async with _build_async_client(cookies=jar, timeout=_timeout(budget_seconds)) as client:
        # 1-2. GET the landing page (normally https://doi.org/{doi}).
        try:
            status, body, final_url = await _get(client, start_url)
        except httpx.HTTPError as exc:
            tried.append(f"GET {start_url} -> {type(exc).__name__}")
            return DirectFetchResult(kind="error", notes=_note("error", tried), tried=tried)

        tried.append(f"GET {start_url} -> {status} ({final_url})")
        wall_seen = _looks_like_wall(status, body)

        # 3c. The landing response itself is already the PDF.
        if _passes_pdf_gate(body):
            return _land(download_dir, doi, body, final_url, tried, note="landing is pdf")

        # 3a/3b. Try up to 3 candidate PDF URLs in priority order. Each candidate
        # may serve the PDF directly, a 202 "generating" (retried in place), or an
        # HTML delivery interstitial (followed ONE hop to its directpdfaccess PDF).
        for url in _candidate_urls(final_url, doi, body)[:3]:
            outcome = await _resolve_candidate(client, url, download_dir, doi, tried)
            if isinstance(outcome, DirectFetchResult):
                return outcome
            if outcome == "wall":
                wall_seen = True

        # 5. Classify the miss.
        kind = "wall" if wall_seen else "no_pdf"
        return DirectFetchResult(kind=kind, final_url=final_url, notes=_note(kind, tried), tried=tried)


async def _fetch_with_generating_retry(
    client: httpx.AsyncClient, url: str, tried: list[str]
) -> tuple[int, bytes, str] | str:
    """GET ``url``, retrying a 202 "generating" a bounded number of times. Returns
    (status, body, final_url), or the string ``"error"`` if the transport failed
    (already appended to ``tried``)."""
    for attempt in range(_GENERATING_RETRIES + 1):
        try:
            status, body, cfinal = await _get(client, url)
        except httpx.HTTPError as exc:
            tried.append(f"GET {url} -> {type(exc).__name__}")
            return "error"
        tried.append(f"GET {url} -> {status} ({cfinal})")
        if status in _GENERATING_STATUSES and attempt < _GENERATING_RETRIES:
            await asyncio.sleep(_GENERATING_BACKOFF_SECONDS)
            continue
        return status, body, cfinal
    return status, body, cfinal


async def _resolve_candidate(
    client: httpx.AsyncClient, url: str, download_dir: Path, doi: str, tried: list[str]
) -> DirectFetchResult | str | None:
    """Resolve one candidate to a landed PDF, a wall, or a miss. Returns a
    ``DirectFetchResult`` on a successful land, ``"wall"`` if a 403/challenge was
    seen, else ``None``. Handles a single HTML-interstitial hop (Optica) to the
    embedded directpdfaccess PDF."""
    fetched = await _fetch_with_generating_retry(client, url, tried)
    if fetched == "error":
        return None
    status, body, cfinal = fetched
    if _looks_like_wall(status, body):
        return "wall"
    if _passes_pdf_gate(body):
        return _land(download_dir, doi, body, cfinal, tried, note="candidate pdf")

    # HTML interstitial: follow ONE hop to the embedded PDF (never recurse further).
    followup = _interstitial_pdf_url(body.decode("utf-8", "replace"), cfinal)
    if not followup or followup == url:
        return None
    fetched2 = await _fetch_with_generating_retry(client, followup, tried)
    if fetched2 == "error":
        return None
    status2, body2, cfinal2 = fetched2
    if _looks_like_wall(status2, body2):
        return "wall"
    if _passes_pdf_gate(body2):
        return _land(download_dir, doi, body2, cfinal2, tried, note="interstitial pdf")
    return None


async def attempt_direct_fetch(
    *,
    doi: str,
    title: str,
    start_url: str,
    download_dir: Path,
    cdp_http_url: str,
    budget_seconds: float = 20.0,
) -> DirectFetchResult:
    """Tier-1 deterministic direct fetch. Returns a DirectFetchResult and NEVER
    raises: transport/timeout failures classify as ``error``, a reachable landing
    with no obtainable PDF as ``no_pdf``, a 403/challenge as ``wall``. The whole
    attempt is bounded by ``budget_seconds`` (an overall ``asyncio.timeout`` on top
    of per-request httpx timeouts) so a slow/hung publisher can never wedge the
    ladder. ``title`` is accepted for signature parity with the other tiers; tier 1
    keys entirely off the DOI redirect chain."""
    tried: list[str] = []

    try:
        raw_cookies = await export_chrome_cookies(cdp_http_url)
    except Exception:
        raw_cookies = []
    jar = _build_cookie_jar(raw_cookies if isinstance(raw_cookies, list) else [])

    try:
        async with asyncio.timeout(budget_seconds):
            return await _run(
                doi=doi, start_url=start_url, download_dir=Path(download_dir),
                jar=jar, budget_seconds=budget_seconds, tried=tried,
            )
    except TimeoutError:
        return DirectFetchResult(kind="error", notes=_note("error (budget exceeded)", tried), tried=tried)
    except httpx.HTTPError as exc:
        return DirectFetchResult(kind="error", notes=_note(f"error ({type(exc).__name__})", tried), tried=tried)
