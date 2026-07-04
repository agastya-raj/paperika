"""Tier 2 of the paperika fast path (AGA-339): deterministic Playwright over the
managed CDP Chrome — NO LLM.

Runs BEFORE the tier-3 codex executor (``executor.py``). It ports the PDF-finding
heuristics encoded in ``executor.PROMPT_TEMPLATE`` into straight-line code: land on
the article page, dismiss the cookie banner, locate the article-level PDF anchor
(rejecting per-figure decoys), navigate to it, let a delivery interstitial settle,
then fetch the bytes through the page's authenticated session and verify the
``%PDF-`` magic before writing ``<download_dir>/<sanitized-doi>.pdf``.

Security invariants (mirrors the executor prompt): all fetched HTML/URLs are treated
strictly as DATA; only the page's own session is used (this machine's IP-based
institutional access); the bearer token is never touched here; no forms are filled
and no credentials are ever entered. Bytes are validated (magic + 10 KB floor) and
written to ``download_dir`` so app.py's existing containment/window/identity gates
keep owning the decision to stream them to the caller — this tier NEVER returns
bytes to anyone.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
import re
import time
from urllib.parse import urljoin

# Tiers 1 and 2 share ONE sanitizer (byte-identical ``<sanitized-doi>.pdf``
# filenames) and ONE SSRF guard (``is_public_http_url``) so both refuse the same
# internal targets — see direct_fetch for the guard's threat model.
from .direct_fetch import is_public_http_url, sanitize_doi


# --- tunables --------------------------------------------------------------

PDF_MAGIC = b"%PDF-"
MIN_PDF_BYTES = 10_000
MAX_NAVIGATIONS = 4
SETTLE_MS = 800
DELIVERY_TIMEOUT_MS = 10_000  # delivery interstitials: wait up to ~10s for redirect
COOKIE_CLICK_TIMEOUT_MS = 800

# Cookie-consent dismissal (best-effort). The OneTrust id + cc-allow class are the
# two most common publisher banners; the rest are button-text matches.
_COOKIE_SELECTORS: tuple[str, ...] = (
    "#onetrust-accept-btn-handler",
    ".cc-allow",
    "button:has-text('Accept All')",
    "button:has-text('Accept all')",
    "button:has-text('Reject All')",
    "button:has-text('I Accept')",
    "button:has-text('Agree')",
)

# Text on an anchor that is (only) a PDF link: "PDF", "View PDF", "Download PDF",
# "PDF Article", "Get PDF (Full Text)", ... — but NOT a sentence that merely
# contains the word.
_PDF_TEXT_RE = re.compile(
    r"^(view|download|get|read)?\s*pdf(\s*(article|\(?full[- ]?text\)?))?$",
    re.IGNORECASE,
)
# Figure-graphic decoys carry a figure suffix like ...-g001, ...-g009.
_FIGURE_HREF_RE = re.compile(r"-g\d+", re.IGNORECASE)

# Bot-challenge / CAPTCHA markers (Cloudflare interstitial + generic CAPTCHA text).
_CHALLENGE_MARKERS: tuple[str, ...] = (
    "captcha",
    "verify you are human",
    "are you a robot",
    "are you human",
    "unusual traffic",
    "checking your browser",
    "just a moment",
    "cf-chl",
    "challenge-platform",
    "cf-browser-verification",
)


@dataclass
class ScriptedFetchResult:
    """Outcome of a Tier-2 scripted fetch. Mirrors DirectFetchResult (Tier 1)."""

    kind: str  # "downloaded" | "no_pdf" | "wall" | "error"
    file_path: str | None = None
    final_url: str | None = None
    notes: str = ""
    tried: list[str] = field(default_factory=list)


# --- pure, unit-testable helpers ------------------------------------------


def is_challenge_html(html: str) -> bool:
    """True if the page content carries a CAPTCHA / bot-challenge marker."""
    low = (html or "").lower()
    return any(marker in low for marker in _CHALLENGE_MARKERS)


def _is_figure_decoy(href: str, text: str) -> bool:
    h = href.lower()
    t = text.lower()
    if "figure" in h or "figure" in t:
        return True
    if "full size" in t:  # "Download Full Size | PDF" per-figure links
        return True
    return bool(_FIGURE_HREF_RE.search(h))


def _is_pdf_marker(href: str, text: str) -> bool:
    if "pdf" in href.lower():
        return True
    return bool(_PDF_TEXT_RE.match(text.strip()))


def pick_pdf_anchor(anchors: list[tuple[str, str]], base_url: str) -> str | None:
    """Choose the article-level PDF anchor from ``(href, text)`` pairs in DOM order.

    Rejects figure decoys, requires a PDF marker (href contains ``pdf`` or the text
    reads like a bare "PDF" link), skips non-navigational hrefs, resolves each
    survivor against ``base_url``, and (SSRF guard, review fix) returns it only
    when it is an http(s) URL on a public host — an internal-host anchor (the CDP
    control port, a private service) or a ``file://`` path from the untrusted page
    DOM is skipped, not navigated. Returns None if none qualify.
    """
    for raw_href, raw_text in anchors:
        href = (raw_href or "").strip()
        text = (raw_text or "").strip()
        if not href:
            continue
        if href.lower().startswith(("#", "javascript:", "mailto:", "data:")):
            continue
        if _is_figure_decoy(href, text):
            continue
        if _is_pdf_marker(href, text):
            resolved = urljoin(base_url, href)
            if is_public_http_url(resolved):
                return resolved
    return None


def _is_pdf_bytes(body: object) -> bool:
    return (
        isinstance(body, (bytes, bytearray))
        and bytes(body[:5]) == PDF_MAGIC
        and len(body) > MIN_PDF_BYTES
    )


def _expired(deadline: float) -> bool:
    return time.monotonic() >= deadline


# --- Playwright plumbing (thin, monkeypatchable seam for tests) -----------


@asynccontextmanager
async def _connect_cdp(cdp_http_url: str):
    """Connect to the managed CDP Chrome and yield its EXISTING default context.

    Exiting the ``async with`` drops the CDP connection (Chrome + browser stay up);
    we never call ``browser.close()`` and never touch other pages/contexts.
    """
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(cdp_http_url)
        context = browser.contexts[0] if browser.contexts else await browser.new_context()
        yield context
    # connection dropped here; the managed Chrome keeps running.


async def _safe_close(page: object) -> None:
    """Close ONLY our page. Swallow any close error — never let cleanup raise."""
    closer = getattr(page, "close", None)
    if closer is None:
        return
    try:
        await closer()
    except Exception:
        pass


async def _call_if_present(page: object, name: str, *args: object, **kwargs: object) -> None:
    fn = getattr(page, name, None)
    if fn is None:
        return
    try:
        await fn(*args, **kwargs)
    except Exception:
        pass


async def _settle_briefly(page: object) -> None:
    await _call_if_present(page, "wait_for_timeout", SETTLE_MS)


async def _settle_for_delivery(page: object) -> None:
    """Let a "your PDF will open shortly" interstitial set its cookie and redirect:
    wait for the network to go idle (up to ~10s), then a short fixed settle so
    ``page.url`` becomes the real media URL."""
    await _call_if_present(page, "wait_for_load_state", "networkidle", timeout=DELIVERY_TIMEOUT_MS)
    await _call_if_present(page, "wait_for_timeout", SETTLE_MS)


async def _dismiss_cookie_banner(page: object) -> None:
    """Best-effort cookie/consent dismissal; a live banner silently swallows the
    PDF click. Stops after the first selector that clicks successfully."""
    clicker = getattr(page, "click", None)
    if clicker is None:
        return
    for selector in _COOKIE_SELECTORS:
        try:
            await clicker(selector, timeout=COOKIE_CLICK_TIMEOUT_MS)
            return
        except Exception:
            continue


async def _safe_content(page: object) -> str:
    """Read ``page.content()`` tolerating a mid-navigation race. A publisher with a
    multi-hop delivery interstitial (Optica) can still be navigating when we read,
    and Playwright then raises "Unable to retrieve content because the page is
    navigating". Wait for the DOM to settle, and on failure settle once and retry;
    return "" if it still can't be read (an unreadable page is not a bot wall, so
    the caller proceeds instead of collapsing the whole tier to error)."""
    getter = getattr(page, "content", None)
    if getter is None:
        return ""
    for attempt in range(2):
        await _call_if_present(page, "wait_for_load_state", "domcontentloaded", timeout=DELIVERY_TIMEOUT_MS)
        try:
            return await getter()
        except (asyncio.CancelledError, KeyboardInterrupt):
            raise
        except Exception:
            if attempt == 0:
                await _settle_briefly(page)
                continue
            return ""
    return ""


async def _collect_anchors(page: object) -> list[tuple[str, str]]:
    raw = await page.eval_on_selector_all(
        "a[href]",
        "els => els.map(e => [e.getAttribute('href') || '', (e.textContent || '').trim()])",
    )
    anchors: list[tuple[str, str]] = []
    for item in raw or []:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            anchors.append((str(item[0] or ""), str(item[1] or "")))
    return anchors


async def _fetch_bytes(page: object, url: str) -> bytes:
    """Fetch ``url`` through the page's own authenticated session (its cookies +
    this machine's institutional IP). Returns raw bytes (possibly non-PDF)."""
    response = await page.request.get(url)
    body = await response.body()
    return body if isinstance(body, (bytes, bytearray)) else b""


def _write_pdf(
    body: bytes, *, doi: str, download_dir: Path, final_url: str | None, tried: list[str]
) -> ScriptedFetchResult:
    download_dir.mkdir(parents=True, exist_ok=True)
    target = download_dir / f"{sanitize_doi(doi)}.pdf"
    target.write_bytes(bytes(body))
    return ScriptedFetchResult(
        kind="downloaded",
        file_path=str(target),
        final_url=final_url,
        notes="scripted fetch wrote verified %PDF- bytes",
        tried=tried,
    )


# --- the tier-2 entry point -----------------------------------------------


async def _run_scripted(
    *,
    page: object,
    doi: str,
    start_url: str,
    download_dir: Path,
    deadline: float,
    tried: list[str],
) -> ScriptedFetchResult:
    """Core algorithm against an already-open page. Never raises: all Playwright /
    timeout failures collapse to kind="error" (contract)."""
    try:
        if _expired(deadline):
            return ScriptedFetchResult(kind="error", notes="budget exhausted before navigation", tried=tried)

        # 1. land on the article page.
        await page.goto(start_url, wait_until="domcontentloaded")
        tried.append(start_url)
        await _settle_briefly(page)

        # bot-wall fast fail on the landing page.
        landing_html = await _safe_content(page)
        if is_challenge_html(landing_html):
            here = str(page.url)
            return ScriptedFetchResult(kind="wall", final_url=here, notes=f"bot/CAPTCHA wall at {here}", tried=tried)

        # 2. dismiss any cookie-consent overlay.
        await _dismiss_cookie_banner(page)

        # 3. locate the article-level PDF anchor.
        anchors = await _collect_anchors(page)
        pdf_url = pick_pdf_anchor(anchors, str(page.url))
        if not pdf_url:
            return ScriptedFetchResult(
                kind="no_pdf", final_url=str(page.url),
                notes="no article-level PDF link on the rendered page", tried=tried,
            )

        if _expired(deadline):
            return ScriptedFetchResult(kind="error", notes="budget exhausted before PDF navigation", tried=tried)

        # 4. navigate to the PDF link and let any delivery interstitial settle.
        # start(1) + pdf(2) navigations — well under MAX_NAVIGATIONS; guard anyway.
        if len(tried) >= MAX_NAVIGATIONS:
            return ScriptedFetchResult(kind="error", notes="navigation budget exceeded", tried=tried)
        await page.goto(pdf_url, wait_until="domcontentloaded")
        tried.append(pdf_url)
        await _settle_for_delivery(page)

        media_url = str(page.url)

        # bot-wall can also appear on the delivery hop.
        delivery_html = await _safe_content(page)
        if is_challenge_html(delivery_html):
            return ScriptedFetchResult(kind="wall", final_url=media_url, notes=f"bot/CAPTCHA wall at {media_url}", tried=tried)

        if _expired(deadline):
            return ScriptedFetchResult(kind="error", notes="budget exhausted before fetch", tried=tried)

        # 5. fetch the settled media URL through the page's session; accept on magic.
        body = await _fetch_bytes(page, media_url)
        tried.append(media_url)
        if _is_pdf_bytes(body):
            return _write_pdf(body, doi=doi, download_dir=download_dir, final_url=media_url, tried=tried)

        # 6. one retry: the interstitial served HTML — fetch the anchor URL directly.
        if pdf_url != media_url and not _expired(deadline):
            body = await _fetch_bytes(page, pdf_url)
            tried.append(pdf_url)
            if _is_pdf_bytes(body):
                return _write_pdf(body, doi=doi, download_dir=download_dir, final_url=pdf_url, tried=tried)

        return ScriptedFetchResult(
            kind="no_pdf", final_url=media_url,
            notes="PDF link did not yield %PDF- bytes", tried=tried,
        )
    except (asyncio.CancelledError, KeyboardInterrupt):
        raise
    except Exception as exc:  # contract: never raise — classify as error.
        return ScriptedFetchResult(kind="error", notes=f"scripted fetch failed: {exc!r}", tried=tried)


async def attempt_scripted_fetch(
    *,
    doi: str,
    title: str,
    start_url: str,
    download_dir: Path,
    cdp_http_url: str,
    budget_seconds: float = 45.0,
) -> ScriptedFetchResult:
    """Tier-2 deterministic fetch. Opens a NEW page on the existing CDP context,
    runs the scripted algorithm, and always closes ONLY that page. Never raises."""
    deadline = time.monotonic() + budget_seconds
    tried: list[str] = []
    try:
        async with _connect_cdp(cdp_http_url) as context:
            page = await context.new_page()
            try:
                return await _run_scripted(
                    page=page, doi=doi, start_url=start_url,
                    download_dir=download_dir, deadline=deadline, tried=tried,
                )
            finally:
                await _safe_close(page)
    except (asyncio.CancelledError, KeyboardInterrupt):
        raise
    except Exception as exc:  # connection / unexpected failure — still never raise.
        return ScriptedFetchResult(kind="error", notes=f"scripted fetch aborted: {exc!r}", tried=tried)
