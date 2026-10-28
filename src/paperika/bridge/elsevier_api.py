"""Elsevier Article Retrieval API step (AGA-1462) for 10.1016/ DOIs.

ScienceDirect always CAPTCHA-walls the browser tiers; the official API returns the
full PDF for IP-entitled callers (the gpu host is on the campus range). This is a
deterministic strategy that runs BEFORE tier 1 and only ever succeeds or misses:
every miss falls through to the normal ladder, never a verdict.

The API key goes ONLY in the ``X-ELS-APIKey`` header of the first request to
api.elsevier.com. A redirect target is fetched with a header-less client through the
shared SSRF guard (``direct_fetch._get``); the key is never logged or put in a
result/notes string.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import logging
from pathlib import Path
from urllib.parse import quote

import httpx

from . import direct_fetch
from .direct_fetch import DirectFetchResult

log = logging.getLogger("paperika.bridge.elsevier_api")

ELSEVIER_DOI_PREFIX = "10.1016/"
API_URL = "https://api.elsevier.com/content/article/doi/"
BUDGET_SECONDS = 30.0
_RATE_HEADER_PREFIXES = ("x-els-status", "x-ratelimit-")


def applies_to(doi: str) -> bool:
    return (doi or "").strip().lower().startswith(ELSEVIER_DOI_PREFIX)


def _rate_headers(headers: httpx.Headers) -> dict[str, str]:
    return {k: v for k, v in headers.items() if k.lower().startswith(_RATE_HEADER_PREFIXES)}


def _miss(notes: str, kind: str = "no_pdf") -> DirectFetchResult:
    return DirectFetchResult(kind=kind, notes=f"elsevier_api: {notes}")


async def _fetch(
    *, doi: str, api_key: str, download_dir: Path, set_key_invalid: Callable[[bool], None],
) -> DirectFetchResult:
    timeout = httpx.Timeout(BUDGET_SECONDS, connect=10.0)
    kwargs: dict = {
        "follow_redirects": False,
        "timeout": timeout,
        "headers": {"Accept": "application/pdf"},
    }
    if direct_fetch._TEST_TRANSPORT is not None:
        kwargs["transport"] = direct_fetch._TEST_TRANSPORT
    url = API_URL + quote(doi, safe="/")
    async with httpx.AsyncClient(**kwargs) as client:
        async with client.stream("GET", url, headers={"X-ELS-APIKey": api_key}) as resp:
            status = resp.status_code
            location = resp.headers.get("location") if resp.is_redirect else None
            rate_info = _rate_headers(resp.headers) if status == 429 else {}
            # Key health tracks ONLY the keyed request to api.elsevier.com: a 401
            # latches, any 2xx/3xx (auth accepted) clears — before the PDF gate.
            if status == 401:
                log.warning("elsevier_api: API key rejected (401); refresh ELSEVIER_API_KEY")
                set_key_invalid(True)
            elif 200 <= status < 400:
                set_key_invalid(False)
            body = b"" if (location or status != 200) else await direct_fetch._read_capped(resp)
            final_url = str(resp.url)
        if location:
            # Never send the key to another host: a clean request, SSRF-guarded.
            target = str(httpx.URL(url).join(location))
            status, body, final_url = await direct_fetch._get(client, target)
    if status == 401:
        return _miss("HTTP 401 from the redirect target" if location else "key rejected (401)")
    if status == 429:
        log.warning("elsevier_api: quota/rate limited (429) %s", rate_info)
        return _miss("rate limited (429)")
    if status != 200:
        return _miss(f"HTTP {status}")
    if not direct_fetch._passes_pdf_gate(body):
        return _miss("200 but not a PDF")
    return direct_fetch._land(download_dir, doi, body, final_url, [], note="elsevier api")


async def attempt_elsevier_fetch(
    *,
    doi: str,
    title: str,
    start_url: str,
    download_dir: Path,
    cdp_http_url: str,
    budget_seconds: float = BUDGET_SECONDS,
    api_key: str,
    set_key_invalid: Callable[[bool], None],
) -> DirectFetchResult:
    """Fetch the PDF via the Article Retrieval API. NEVER raises; every miss is
    ``no_pdf``/``error`` (never ``wall``, so it cannot feed bot-wall evidence)."""
    try:
        async with asyncio.timeout(budget_seconds):
            return await _fetch(
                doi=doi, api_key=api_key, download_dir=Path(download_dir),
                set_key_invalid=set_key_invalid,
            )
    except TimeoutError:
        return _miss("budget exceeded", "error")
    except Exception as exc:  # noqa: BLE001 - any failure is a miss; the ladder continues
        # Type name only: exception text can carry header values (the key).
        return _miss(type(exc).__name__, "error")
