"""Managed-Chrome health: CDP ws-connect probe (cached), orphan-target cleanup,
and lock-guarded restart/recycle (§2.5).

The trial showed plain HTTP ``/json/version`` can answer while the websocket
connect path is wedged by an orphaned client, so the real liveness check is a
Playwright ``connect_over_cdp`` probe. The probe result is cached 30 s so neighbor
``/healthz`` polling can't churn CDP connects (≤ 2 ws-connects/min).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import subprocess
import urllib.request

CDP_HOST = "127.0.0.1"
CDP_PORT = 9224
CDP_HTTP = f"http://{CDP_HOST}:{CDP_PORT}"
CHROME_UNIT = "paperika-chrome.service"

PROBE_CACHE_TTL_SECONDS = 30
PROBE_CONNECT_TIMEOUT_MS = 5000
RESTART_WAIT_SECONDS = 30
IDLE_RECYCLE_SECONDS = 24 * 60 * 60

# The managed Chrome's only standing page; everything else is an executor orphan.
_STANDING_URL = "about:blank"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class ProbeResult:
    ok: bool
    version: str | None = None
    checked_at: datetime | None = None
    error: str | None = None

    def age_seconds(self, now: datetime | None = None) -> float | None:
        if self.checked_at is None:
            return None
        return ((now or _utc_now()) - self.checked_at).total_seconds()


# Module-level probe cache (single-process bridge — --workers 1).
_cached_probe: ProbeResult | None = None


async def _ws_connect_probe(cdp_http: str = CDP_HTTP) -> ProbeResult:
    """Real Playwright CDP ws-connect probe. Connecting then closing a CDP-attached
    browser only drops the connection — Chrome stays up."""
    try:
        from playwright.async_api import async_playwright
    except Exception as exc:  # pragma: no cover - playwright always present in venv
        return ProbeResult(False, checked_at=_utc_now(), error=f"playwright import failed: {exc}")

    try:
        async with async_playwright() as p:
            browser = await p.chromium.connect_over_cdp(cdp_http, timeout=PROBE_CONNECT_TIMEOUT_MS)
            version = browser.version
            await browser.close()
        return ProbeResult(True, version=version, checked_at=_utc_now())
    except Exception as exc:
        return ProbeResult(False, checked_at=_utc_now(), error=str(exc))


async def probe(*, fresh: bool = False, cdp_http: str = CDP_HTTP) -> ProbeResult:
    """Cached CDP probe. ``fresh=True`` bypasses the cache (the /download
    pre-flight always fresh-probes; /healthz serves the cache within 30 s)."""
    global _cached_probe
    now = _utc_now()
    if not fresh and _cached_probe is not None:
        age = _cached_probe.age_seconds(now)
        if age is not None and age < PROBE_CACHE_TTL_SECONDS:
            return _cached_probe
    result = await _ws_connect_probe(cdp_http)
    _cached_probe = result
    return result


def reset_probe_cache() -> None:
    global _cached_probe
    _cached_probe = None


def _http_json(path: str, *, timeout: float = 5.0) -> object:
    with urllib.request.urlopen(f"{CDP_HTTP}{path}", timeout=timeout) as resp:  # noqa: S310
        return json.loads(resp.read().decode("utf-8"))


def _http_get(path: str, *, timeout: float = 5.0) -> None:
    with urllib.request.urlopen(f"{CDP_HTTP}{path}", timeout=timeout) as resp:  # noqa: S310
        resp.read()


async def close_orphan_targets(cdp_http: str = CDP_HTTP) -> list[str]:
    """Close every page target whose URL is not the standing about:blank — these
    are pages a dead executor left behind. Returns the closed target ids."""
    loop = asyncio.get_running_loop()

    def _list_and_close() -> list[str]:
        targets = _http_json("/json/list")
        closed: list[str] = []
        if not isinstance(targets, list):
            return closed
        for t in targets:
            if not isinstance(t, dict):
                continue
            if t.get("type") != "page":
                continue
            url = (t.get("url") or "").strip()
            if url == _STANDING_URL:
                continue
            tid = t.get("id")
            if not tid:
                continue
            try:
                _http_get(f"/json/close/{tid}")
                closed.append(str(tid))
            except Exception:
                pass
        return closed

    return await loop.run_in_executor(None, _list_and_close)


def chrome_active_since() -> datetime | None:
    """ActiveEnterTimestamp of the chrome unit, or None if unavailable."""
    try:
        out = subprocess.run(
            ["systemctl", "show", "-p", "ActiveEnterTimestamp", "--value", CHROME_UNIT],
            capture_output=True, text=True, timeout=10, check=False,
        )
    except Exception:
        return None
    raw = (out.stdout or "").strip()
    if not raw:
        return None
    # systemd format e.g. "Sat 2026-06-13 10:00:00 UTC"
    for fmt in ("%a %Y-%m-%d %H:%M:%S %Z", "%Y-%m-%d %H:%M:%S %Z"):
        try:
            dt = datetime.strptime(raw, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


def _restart_chrome_unit() -> bool:
    """sudo -n systemctl restart paperika-chrome. -n so it can never hang on a
    password prompt. Safe in-handler ONLY because the bridge unit declares
    Wants= (not Requires=) on paperika-chrome (§2.2)."""
    try:
        out = subprocess.run(
            ["sudo", "-n", "systemctl", "restart", CHROME_UNIT],
            capture_output=True, text=True, timeout=30, check=False,
        )
        return out.returncode == 0
    except Exception:
        return False


async def _wait_for_cdp(max_seconds: int = RESTART_WAIT_SECONDS) -> bool:
    deadline = _utc_now() + timedelta(seconds=max_seconds)
    while _utc_now() < deadline:
        result = await _ws_connect_probe()
        if result.ok:
            global _cached_probe
            _cached_probe = result
            return True
        await asyncio.sleep(2)
    return False


@dataclass(slots=True)
class RecoveryResult:
    recovered: bool
    action: str  # "closed_targets" | "restarted_chrome" | "unrecovered" | "noop"


async def recover() -> RecoveryResult:
    """Wedge-recovery routine (§2.5). Invoked after any timeout/kill/gave_up/
    unparseable outcome (before lock release) and once on a failed pre-flight probe.

    1. Close orphan page targets via CDP HTTP.
    2. Re-probe (real ws-connect).
    3. Escalate: if /json/list itself failed or the re-probe still fails ⇒
       sudo -n systemctl restart paperika-chrome, then poll the ws probe up to 30 s.
    """
    loop = asyncio.get_running_loop()
    list_failed = False
    try:
        await close_orphan_targets()
    except Exception:
        list_failed = True

    if not list_failed:
        result = await probe(fresh=True)
        if result.ok:
            return RecoveryResult(True, "closed_targets")

    restarted = await loop.run_in_executor(None, _restart_chrome_unit)
    if not restarted:
        return RecoveryResult(False, "unrecovered")
    if await _wait_for_cdp():
        return RecoveryResult(True, "restarted_chrome")
    return RecoveryResult(False, "unrecovered")


async def idle_recycle_if_stale(*, threshold_seconds: int = IDLE_RECYCLE_SECONDS) -> bool:
    """Lock-guarded idle recycle (§2.3 step 5): if chrome has been up longer than
    the threshold, restart it now (lock held ⇒ never mid-download), wait for CDP.
    Returns True if a recycle happened."""
    since = chrome_active_since()
    if since is None:
        return False
    uptime = (_utc_now() - since).total_seconds()
    if uptime < threshold_seconds:
        return False
    loop = asyncio.get_running_loop()
    if await loop.run_in_executor(None, _restart_chrome_unit):
        await _wait_for_cdp()
    return True
