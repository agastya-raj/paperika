"""paperika institutional-download bridge — FastAPI app (§2).

Manual/on-demand only: the bridge acts only on inbound HTTP, never polls. A
single in-process asyncio.Lock serializes /download and /selftest/codex; the unit
runs --workers 1 so the lock is global. Rate discipline + the write-ahead attempt
record live in limits.py; Chrome health in chrome.py; the codex executor in
executor.py.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import time
from typing import Any
from urllib.parse import urlparse

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

from ..config import PaperikaConfig
from ..db import Database
from ..downloader import Downloader
from ..models import LocateCandidate, ManualIntervention, ParsedInput
from ..notifications import NotificationEvent, emit_notification_event
from . import chrome, executor, limits
from .direct_fetch import attempt_direct_fetch
from .executor import run_claude, run_codex
from .scripted_browser import attempt_scripted_fetch

logger = logging.getLogger(__name__)

# --- constants -------------------------------------------------------------

MAX_FILE_SIZE = 500 * 1024 * 1024  # mirror KC citations/service.py MAX_FILE_SIZE
TOKEN_ENV = "PAPERIKA_BRIDGE_TOKEN"
DOI_RE = re.compile(r"^10\.\d{4,9}/\S+$")
TITLE_MAX = 300
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")
SALVAGE_GRACE_SECONDS = 10
SWEEP_WINDOW_SECONDS = executor.EXEC_WALL_SECONDS + 30

# One deadline for the whole /download ladder (AGA-1460). KC's client timeout is
# 1200 s (knowledge-center sources/paperika.py paperika_timeout), so the bridge
# must answer before it: TIER1 + TIER2 + codex's own wall (60+45+900 = 1005 s)
# always fits, and the claude FALLBACK only starts when at least
# FALLBACK_MIN_SECONDS remain, with its wall capped at remaining - FALLBACK_TAIL_SECONDS.
REQUEST_BUDGET_SECONDS = 1140
FALLBACK_MIN_SECONDS = 180
FALLBACK_TAIL_SECONDS = 30
MONOTONIC = time.monotonic  # module attribute so tests can stub the clock

# Per-tier wall-clock budgets for the deterministic ladder (AGA-339). Tier 3
# (codex) is bounded separately by executor.EXEC_WALL_SECONDS.
# 20.0 was under the real cost of a tier-1 hit: the Optica flow (challenge
# handshake → replay → interstitial → PDF) measures ~30-38s end to end, of which
# ~29s is the 11.6 MB transfer itself and only ~1.4s the gate — so the old budget
# killed a WORKING fetch as "error (budget exceeded)" (AGA-489). This is an overall
# asyncio.timeout; direct_fetch._timeout() separately caps each REQUEST's read at
# min(budget, 15.0) — a per-read gap cap, not a total, so a steady multi-minute
# stream never trips it.
TIER1_BUDGET = 60.0
# AGA-1256: the longest tier-1 cooldown the ladder waits out instead of skipping
# tier 1 (direct_fetch spacing is 30 s, so this is cooldown + margin). SLEEP is
# a module attribute so tests can stub the wait.
TIER1_COOLDOWN_WAIT_MAX = 45
SLEEP = asyncio.sleep
TIER2_BUDGET = 45.0

# GET /requests/{id}/screenshot (AGA-596 P3) media type by file extension. Bridge
# screenshots are codex's final.png; the downloader.py queue path (a separate
# consumer of the same schema) can also leave a .txt fallback when Playwright is
# unavailable, so this stays a lookup rather than a hardcoded image/png.
_SCREENSHOT_MEDIA_TYPES: dict[str, str] = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# --- request/response models ----------------------------------------------


class DownloadRequest(BaseModel):
    doi: str
    title: str
    source_url: str | None = None
    requested_by: str = "kc"


class BridgeState(BaseModel):
    sandbox_mode: str | None = None
    last_selftest_ok: bool | None = None
    last_selftest_at: str | None = None
    last_variant1_failure_reason: str | None = None
    last_recovery_action: str | None = None
    claude_last_selftest_ok: bool | None = None
    claude_last_selftest_at: str | None = None


# --- bridge state (persisted) ---------------------------------------------


class StateStore:
    def __init__(self, path: Path) -> None:
        self._path = path

    def load(self) -> BridgeState:
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return BridgeState()
        return BridgeState(**{k: v for k, v in data.items() if k in BridgeState.model_fields})

    def save(self, state: BridgeState) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(state.model_dump(), indent=2), encoding="utf-8")

    def update(self, **kwargs: Any) -> BridgeState:
        state = self.load()
        for k, v in kwargs.items():
            setattr(state, k, v)
        self.save(state)
        return state


# --- failure taxonomy → HTTP (§2.7) ---------------------------------------

# Maps an ExecResult.kind (or salvage/verify outcome) to (attempt outcome,
# event_type, http_status, error_code).
_TAXONOMY: dict[str, tuple[str, str, int, str]] = {
    "wrong_paper": ("wrong_paper", "verification_failed", 502, "wrong_paper"),
    "throttled": ("throttled", "download_throttled", 429, "throttled"),
    "paywalled_no_access": ("no_access", "no_institutional_access", 403, "paywalled_no_access"),
    "bot_wall": ("bot_wall", "bot_wall", 502, "bot_wall"),
    # Both deterministic tiers walled and codex was unavailable to classify — a
    # DISTINCT error_code from codex-confirmed "bot_wall" (AGA-596 P1), still a
    # retryable 502. KC has carried it in RETRYABLE_CODES since AGA-596 P1
    # (knowledge-center sources/paperika.py). The notification event_type stays
    # the documented "bot_wall" (unchanged contract) — only the HTTP error_code
    # differs.
    "bot_wall_unclassified": (
        "bot_wall_unclassified", "bot_wall", 502, "bot_wall_unclassified",
    ),
    "gave_up": ("executor_gave_up", "executor_failed", 502, "codex_gave_up"),
    # A death, not a verdict (AGA-489): the executor crashed / failed its turn /
    # exited non-zero, so it never judged the paper. Distinct from gave_up — which
    # is terminal advice ("this paper is not obtainable, stop") — because the same
    # underlying event used to be stamped gave_up when it self-exited and timeout
    # when it hit the wall, i.e. the CLEANER death got the more terminal code.
    # RETRYABLE: the caller should try again (KC keys off this error_code string).
    "executor_died": ("executor_died", "executor_failed", 502, "executor_died"),
    "auth_error": ("auth_error", "executor_failed", 503, "codex_auth"),
    "timeout": ("timeout", "executor_failed", 504, "executor_timeout"),
    # AGA-1460: codex's ChatGPT plan is out of usage. Retryable (the claude fallback
    # normally absorbs it before the caller sees it).
    "codex_quota": ("codex_quota", "executor_failed", 502, "codex_quota"),
    # AGA-1460: the claude fallback's API key was rejected. Operator (KC:
    # NEEDS_OPERATOR_CODES), like codex_auth.
    "claude_auth": ("claude_auth", "executor_failed", 503, "claude_auth"),
    # AGA-1460: the claude run hit its per-run --max-budget-usd. Retryable.
    "claude_budget": ("claude_budget", "executor_failed", 502, "claude_budget"),
    # AGA-1457: an UNEXPECTED exception escaped the ladder (a bug, not a verdict).
    # 500, non-retryable, operator attention (KC: NEEDS_OPERATOR_CODES). Not an
    # expected-failure channel: expected failures map to the kinds above.
    "bridge_error": ("bridge_error", "executor_failed", 500, "bridge_error"),
}


def _json_error(status: int, error_code: str, detail: str = "", **extra: Any) -> JSONResponse:
    body = {"error_code": error_code, "detail": detail}
    body.update(extra)
    return JSONResponse(status_code=status, content=body)


# --- the bridge ------------------------------------------------------------


@dataclass
class Bridge:
    config: PaperikaConfig
    db: Database
    downloader: Downloader
    state_store: StateStore
    lock: asyncio.Lock

    # --- boundary sanitization (§2.4 layer 2) ---

    @staticmethod
    def normalize_doi(raw: str) -> str | None:
        value = (raw or "").strip().lower()
        for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
            if value.startswith(prefix):
                value = value[len(prefix):]
        value = value.strip().rstrip(".,;)]")
        if not value or not DOI_RE.match(value):
            return None
        return value

    @staticmethod
    def sanitize_title(raw: str) -> str:
        text = _CONTROL_CHARS.sub(" ", raw or "")
        text = " ".join(text.split())
        return text[:TITLE_MAX]

    @staticmethod
    def sanitize_source_url(raw: str | None) -> tuple[bool, str | None]:
        if raw is None:
            return True, None
        parsed = urlparse(raw.strip())
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return False, None
        return True, raw.strip()

    # --- dedupe (§2.3 step 2) ---

    def dedupe_hit(self, *, doi: str, title: str) -> Path | None:
        row = self.db.find_verified_pdf(doi=doi, title=title)
        if not row:
            return None
        path_str = row["local_pdf_path"]
        if not path_str:
            return None
        path = Path(path_str)
        return path if path.exists() else None

    # --- locate + verify (§2.6) ---

    def _locate_in_window(self, window_start: datetime, window_end: datetime) -> Path | None:
        downloads = self.config.download_dir
        if not downloads.exists():
            return None
        start_ts = (window_start - timedelta(seconds=SALVAGE_GRACE_SECONDS)).timestamp()
        end_ts = (window_end + timedelta(seconds=SALVAGE_GRACE_SECONDS)).timestamp()
        candidates: list[tuple[float, Path]] = []
        for p in downloads.iterdir():
            if not p.is_file():
                continue
            try:
                mtime = p.stat().st_mtime
            except OSError:
                continue
            if start_ts <= mtime <= end_ts:
                candidates.append((mtime, p))
        if not candidates:
            return None
        candidates.sort(reverse=True)
        return candidates[0][1]

    def _within_download_window(
        self, path: Path, window_start: datetime, window_end: datetime
    ) -> bool:
        """Containment + mtime gate for a codex-REPORTED file_path (review fix,
        finding 1). The window locator already enforces both; the primary
        ``downloaded`` candidate did not, so an injection-steered codex could name
        any agastya-owned, %PDF--prefixed file anywhere on disk and have the
        UNsandboxed bridge link it (verify-pass) or path.replace() it into
        quarantine (verify-fail destructive primitive). Require: (a) the resolved
        path lives under config.download_dir, and (b) its mtime falls inside the
        run window (same grace as _locate_in_window)."""
        try:
            resolved = path.resolve()
            downloads = self.config.download_dir.resolve()
        except OSError:
            return False
        if downloads not in resolved.parents:
            return False
        try:
            mtime = resolved.stat().st_mtime
        except OSError:
            return False
        start_ts = (window_start - timedelta(seconds=SALVAGE_GRACE_SECONDS)).timestamp()
        end_ts = (window_end + timedelta(seconds=SALVAGE_GRACE_SECONDS)).timestamp()
        return start_ts <= mtime <= end_ts

    def _mechanical_gates(self, path: Path) -> bool:
        try:
            if not path.is_file():
                return False
            size = path.stat().st_size
            if size <= 0 or size > MAX_FILE_SIZE:
                return False
            with path.open("rb") as fh:
                head = fh.read(5)
            return head == b"%PDF-"
        except OSError:
            return False

    def verify_identity(self, *, doi: str, title: str, path: Path) -> tuple[bool, str]:
        parsed = ParsedInput(raw_input=doi or title or "", title=title or None, doi=doi or None)
        check = self.downloader._verify_downloaded_pdf_identity(parsed, path)
        return bool(check.ok), check.reason

    @staticmethod
    def _sha256(path: Path) -> str:
        h = hashlib.sha256()
        with path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()

    def link_paper(self, *, doi: str, title: str, path: Path, final_url: str | None) -> int:
        candidate = LocateCandidate(
            title=title or None,
            doi=doi or None,
            canonical_url=final_url,
            pdf_url=final_url,
            source="paperika_bridge",
            confidence=1.0,
        )
        paper_id = self.db.upsert_located_paper(candidate)
        self.db.mark_paper_downloaded(paper_id, str(path))
        return paper_id

    # --- notification helper ---

    def emit(
        self,
        *,
        event_type: str,
        request_id: int,
        paper_id: int | None,
        status_after: str,
        message: str,
        status_before: str | None = None,
    ) -> None:
        event = NotificationEvent(
            event_type=event_type,
            request_id=request_id,
            paper_id=paper_id,
            status_before=status_before,
            status_after=status_after,
            message=message,
        )
        try:
            emit_notification_event(self.config, event)
        except OSError:
            pass

    # --- write-ahead attempt bookkeeping (raw SQL through db.transaction) ---

    def write_ahead(
        self, *, request_id: int, paper_id: int | None, run_dir: str,
        strategy: str = "codex_bridge",
    ) -> int:
        with self.db.transaction() as conn:
            return limits.write_ahead_attempt(
                conn, request_id=request_id, paper_id=paper_id, run_dir=run_dir, strategy=strategy
            )

    def resolve_attempt(self, attempt_id: int, **kwargs: Any) -> None:
        with self.db.transaction() as conn:
            limits.resolve_attempt(conn, attempt_id, **kwargs)

    def wall_evidence(
        self, request_id: int, *, exclude_attempt_id: int | None = None
    ) -> tuple[list[str], str | None, str | None]:
        """Bot-wall evidence for this request, read back from ``paper_attempts``
        — durable (survives a bridge restart) rather than an in-memory value
        computed only during the single /download call that ran the tiers.
        Matches BOTH a deterministic tier's ``outcome='wall'`` AND codex's own
        confirmed wall (``outcome='bot_wall'`` — a DIFFERENT string, since
        codex's terminal taxonomy resolves its attempt with the kind name
        itself, see ``_TAXONOMY``). Missing the latter (review round-3 finding
        2) meant a crash between codex's attempt being resolved and the
        request's own status update left the sweep with NO evidence at all,
        even though codex had already confirmed the wall with its own URL and
        screenshot. Returns ``(walled/confirming strategies in run order, the
        LAST such row's current_url, that row's screenshot_path)`` — a tier's
        ``current_url``/``screenshot_path`` is persisted onto its own attempt
        row at resolve time (see ``_run_deterministic_tier`` / ``_finish_failure``),
        so this survives even if the bridge crashes before the request itself
        is updated. ``exclude_attempt_id``: a live finish (``_finish_failure``)
        calls ``resolve_attempt`` for ITS OWN attempt (which may itself just
        have been written ``outcome='bot_wall'``) before calling this — pass
        that attempt's id so a codex-confirmed wall doesn't self-report as its
        own "prior" evidence."""
        with self.db.transaction() as conn:
            rows = conn.execute(
                "SELECT strategy, current_url, screenshot_path FROM paper_attempts "
                "WHERE request_id = ? AND outcome IN ('wall', 'bot_wall') AND id != ? "
                "ORDER BY id ASC",
                (request_id, exclude_attempt_id if exclude_attempt_id is not None else -1),
            ).fetchall()
        walled_tiers = [row["strategy"] for row in rows if row["strategy"]]
        last_wall_url = rows[-1]["current_url"] if rows else None
        last_screenshot = rows[-1]["screenshot_path"] if rows else None
        return walled_tiers, last_wall_url, last_screenshot

    # --- screenshot lookup + containment (AGA-596 P3) ---

    def _screenshot_allowed_roots(self) -> list[Path]:
        """Roots a served screenshot must resolve inside: the downloader's failure-
        screenshot dir and the bridge's own codex run-dir tree (where codex's
        final.png lands). Mirrors the containment idiom in
        ``_within_download_window`` — never trust a DB-stored path blindly."""
        return [
            self.config.screenshot_dir.resolve(),
            (self.config.db_path.parent / "codex_runs").resolve(),
        ]

    def resolve_screenshot_path(self, raw: str) -> Path | None:
        try:
            resolved = Path(raw).resolve()
        except OSError:
            return None
        if not any(root in resolved.parents for root in self._screenshot_allowed_roots()):
            return None
        return resolved if resolved.is_file() else None

    def latest_attempt_screenshot(self, request_id: int) -> str | None:
        """The most recent non-null ``paper_attempts.screenshot_path`` for this
        request — fallback when the request itself carries no
        ``manual_screenshot_path`` (e.g. an older request predating AGA-596 P2)."""
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT screenshot_path FROM paper_attempts "
                "WHERE request_id = ? AND screenshot_path IS NOT NULL "
                "ORDER BY id DESC LIMIT 1",
                (request_id,),
            ).fetchone()
        return row["screenshot_path"] if row else None


# --- auth dependency (§2.2) -----------------------------------------------


def _configured_token() -> str:
    return os.environ.get(TOKEN_ENV, "")


def require_token(request: Request) -> JSONResponse | None:
    """Returns a JSONResponse to short-circuit on failure, or None to proceed."""
    server_token = _configured_token()
    if not server_token:
        return _json_error(503, "token_unset", "server bearer token not configured")
    header = request.headers.get("authorization", "")
    presented = header[7:] if header.lower().startswith("bearer ") else ""
    if not presented or not hmac.compare_digest(presented, server_token):
        return _json_error(401, "unauthorized", "missing or invalid bearer token")
    return None


# --- app factory -----------------------------------------------------------


def build_bridge(config: PaperikaConfig | None = None) -> Bridge:
    config = config or PaperikaConfig.from_env()
    config.chrome_cdp_url = chrome.CDP_HTTP
    config.ensure_runtime_dirs()
    (config.db_path.parent / "codex_runs").mkdir(parents=True, exist_ok=True)
    db = Database.from_config(config)
    # Registered BEFORE init() so a DB file rebuilt later by connect() (AGA-1250)
    # gets the bridge columns too, not just the shared schema.
    db.schema_hooks.append(limits.ensure_bridge_columns)
    db.init()
    with db.transaction() as conn:
        limits.ensure_bridge_columns(conn)
    downloader = Downloader(config, db)
    state_path = config.db_path.parent / "bridge_state.json"
    return Bridge(
        config=config,
        db=db,
        downloader=downloader,
        state_store=StateStore(state_path),
        lock=asyncio.Lock(),
    )


def _paperika_version() -> str:
    try:
        out = subprocess.run(
            ["git", "-C", "/home/agastya/paperika", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        return (out.stdout or "").strip() or "unknown"
    except Exception:
        return "unknown"


def startup_sweep(bridge: Bridge) -> None:
    """On every start: salvage-first over stale outcome='running' rows (§2.3),
    then reap any request left dangling 'in_progress' with no running attempt at
    all (review round-2 finding 1) — a crash AFTER an attempt is resolved but
    BEFORE the request itself receives its terminal status transition (or before
    the next tier's attempt row is even created) otherwise leaves the request
    in_progress forever, with any wall evidence trapped in paper_attempts."""
    with bridge.db.transaction() as conn:
        rows = limits.running_attempts(conn)
    for row in rows:
        request_id = row["request_id"]
        started = row["started_at"]
        # Recover doi/title from the request row for the salvage verify.
        req = bridge.db.get_request(request_id)
        doi = (req["inferred_doi"] if req else None) or ""
        title = (req["inferred_title"] if req else None) or ""
        window_start = _parse_iso(started) or _utc_now()
        window_end = window_start + timedelta(seconds=SWEEP_WINDOW_SECONDS)
        if window_end > _utc_now():
            window_end = _utc_now()

        salvaged = False
        if doi or title:
            located = bridge._locate_in_window(window_start, window_end)
            if located and bridge._mechanical_gates(located):
                ok, _reason = bridge.verify_identity(doi=doi, title=title, path=located)
                if ok:
                    paper_id = bridge.link_paper(doi=doi, title=title, path=located, final_url=None)
                    bridge.resolve_attempt(
                        row["id"], outcome="completed", paper_id=paper_id,
                        message="salvaged_after_interrupt",
                    )
                    bridge.db.update_request_status(request_id, "completed", paper_id=paper_id)
                    bridge.emit(
                        event_type="paper_downloaded", request_id=request_id, paper_id=paper_id,
                        status_after="completed", status_before="in_progress",
                        message="salvaged after bridge restart (subtype=salvaged)",
                    )
                    salvaged = True
        if not salvaged:
            bridge.resolve_attempt(row["id"], outcome="interrupted", message="bridge restarted mid-run")
            # If some OTHER, already-resolved attempt for this request (a
            # deterministic tier, or codex itself) recorded a wall before THIS
            # row's strategy was interrupted by the restart, that evidence must
            # not be lost (review round-2 finding 1) — it is durable in
            # paper_attempts, so surface it here too.
            walled_tiers, last_wall_url, last_screenshot = bridge.wall_evidence(request_id)
            manual = None
            if walled_tiers:
                current_url = _preferred_current_url(bridge, request_id, last_wall_url)
                manual = ManualIntervention(
                    reason=(
                        f"{' and '.join(walled_tiers)} hit a bot/CAPTCHA wall; the bridge "
                        f"restarted while {row['strategy']} was running, before the request "
                        "could reach a terminal status"
                    ),
                    screenshot_path=last_screenshot,
                    page_title=None,
                    current_url=current_url,
                    suggested_next_action=_wall_suggested_action(current_url),
                )
            try:
                applied = bridge.db.update_request_status(request_id, "failed", manual=manual)
            except KeyError:
                applied = False
            # Skip the notification when the guard turned this into a no-op
            # (review round-2 finding 2) — status_after="failed" would otherwise
            # misreport a request that is actually still in whatever state it was
            # already in (only possible if it had somehow already reached success).
            if applied:
                bridge.emit(
                    event_type="executor_failed", request_id=request_id, paper_id=row["paper_id"],
                    status_after="failed", status_before="in_progress",
                    message="bridge restarted mid-run (subtype=interrupted)",
                )

    # Second pass: requests stuck 'in_progress' with no terminal status of their
    # own. No "still has a running attempt" exclusion is needed here (review
    # round-3 finding 4): the loop above is synchronous and, for EVERY row it
    # reaps, unconditionally transitions that row's request away from
    # 'in_progress' (to 'completed' or 'failed' — never blocked, since
    # 'in_progress' is never in _SUCCESS_STATUSES) before this query runs. So by
    # construction, any request still 'in_progress' here already has zero
    # running attempts — this is restart-idempotent under the documented
    # single-process startup model (only one sweep runs, synchronously, before
    # the app serves its first request).
    with bridge.db.transaction() as conn:
        stuck_requests = conn.execute(
            "SELECT id FROM paper_requests WHERE status = 'in_progress'"
        ).fetchall()
    for stuck in stuck_requests:
        request_id = stuck["id"]
        walled_tiers, last_wall_url, last_screenshot = bridge.wall_evidence(request_id)
        manual = None
        if walled_tiers:
            current_url = _preferred_current_url(bridge, request_id, last_wall_url)
            manual = ManualIntervention(
                reason=(
                    f"{' and '.join(walled_tiers)} hit a bot/CAPTCHA wall; the bridge "
                    "restarted before the request reached a terminal status"
                ),
                screenshot_path=last_screenshot,
                page_title=None,
                current_url=current_url,
                suggested_next_action=_wall_suggested_action(current_url),
            )
        try:
            applied = bridge.db.update_request_status(request_id, "failed", manual=manual)
        except KeyError:
            applied = False
        if applied:
            bridge.emit(
                event_type="executor_failed", request_id=request_id, paper_id=None,
                status_after="failed", status_before="in_progress",
                message="bridge restarted before the request reached a terminal status "
                        "(subtype=stuck_in_progress)",
            )


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


_STRATEGIES = ("direct_fetch", "scripted_browser", "codex_bridge", "claude_bridge")

# A selftest older than this is treated as stale for the /healthz codex_usable
# signal — codex auth/CLI can rot silently between requests (a revoked ChatGPT
# token still leaves auth.json on disk), so a months-old "ok" is not evidence
# codex works today (AGA-493).
SELFTEST_MAX_AGE_DAYS = 14


def _codex_usable(state: BridgeState) -> bool:
    """Honest 'is the codex tier actually usable right now' derived from the
    readiness latch — the last selftest passed, its sandbox mode is still set
    (not cleared by a runtime auth_error), and the selftest is not stale. This is
    what /healthz reports instead of bare auth.json file-presence (AGA-493)."""
    if not (state.last_selftest_ok and state.sandbox_mode):
        return False
    last_at = _parse_iso(state.last_selftest_at)
    if last_at is None:
        return False
    age = _utc_now() - last_at
    # Require a non-negative age: a future last_selftest_at (clock skew / corrupted
    # state file) would otherwise pass the <= MAX_AGE check and latch codex_usable
    # true until that future instant + MAX_AGE (AGA-493).
    return timedelta(0) <= age <= timedelta(days=SELFTEST_MAX_AGE_DAYS)


def _claude_usable(state: BridgeState) -> bool:
    """Healthz-honest: an API key is available AND a fresh claude selftest passed."""
    if not (state.claude_last_selftest_ok and executor.anthropic_api_key()):
        return False
    last_at = _parse_iso(state.claude_last_selftest_at)
    if last_at is None:
        return False
    return timedelta(0) <= _utc_now() - last_at <= timedelta(days=SELFTEST_MAX_AGE_DAYS)


def _claude_ready(state: BridgeState) -> bool:
    """Fallback gate: a key is available and the last selftest/run did not prove it
    dead. Deliberately does NOT require a selftest — the fallback must work without
    an operator step — but a claude_auth failure latches it off (selftest_ok=False)
    until POST /selftest/claude passes."""
    return bool(executor.anthropic_api_key()) and state.claude_last_selftest_ok is not False


def _db_usable(db: Database) -> bool:
    """True when the state DB answers a query against the schema it should hold.
    /healthz reported ``ok`` for three days while every /download died on
    ``no such table: papers`` (AGA-1250); liveness must include the DB."""
    try:
        with db.transaction() as conn:
            conn.execute("SELECT 1 FROM papers LIMIT 1").fetchall()
            conn.execute("SELECT 1 FROM paper_attempts LIMIT 1").fetchall()
        return True
    except sqlite3.Error:
        return False


def create_app(config: PaperikaConfig | None = None) -> FastAPI:
    bridge = build_bridge(config)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await asyncio.get_running_loop().run_in_executor(None, startup_sweep, bridge)
        yield

    app = FastAPI(title="paperika-bridge", lifespan=lifespan)
    app.state.bridge = bridge

    # --- /healthz (tiered, probe cached 30s) ---

    @app.get("/healthz")
    async def healthz(request: Request) -> Response:
        token_configured = bool(_configured_token())
        result = await chrome.probe(fresh=False)
        db_ok = _db_usable(bridge.db)
        healthy = result.ok and db_ok
        status = "ok" if healthy else "degraded"
        http_status = 200 if healthy else 503

        # Unauthenticated tier: liveness only.
        header = request.headers.get("authorization", "")
        presented = header[7:] if header.lower().startswith("bearer ") else ""
        authed = bool(token_configured and presented and hmac.compare_digest(presented, _configured_token()))
        if not authed:
            return JSONResponse(
                status_code=http_status,
                content={
                    "status": status,
                    "token_configured": token_configured,
                    "db": "ok" if db_ok else "unavailable",
                },
            )

        state = bridge.state_store.load()
        downloads_today = None
        claude_cost = None
        attempts_by_strategy: dict[str, int | None] = {
            strat: None for strat in _STRATEGIES
        }
        if db_ok:
            with bridge.db.transaction() as conn:
                downloads_today = limits.attempts_today(conn)
                attempts_by_strategy = {
                    strat: limits.attempts_today(conn, strat)
                    for strat in _STRATEGIES
                }
                claude_cost = limits.month_cost_usd(conn, "claude_bridge")
        body = {
            "status": status,
            "db": {"ok": db_ok, "path": str(bridge.config.db_path)},
            "chrome": {
                "cdp_connectable": result.ok,
                "browser": result.version,
                "probe_age_seconds": int(result.age_seconds() or 0),
            },
            "codex": {
                # auth_json_present is a raw signal only — a revoked token still
                # leaves the file, so it does NOT prove codex works. codex_usable
                # is the honest derived answer (AGA-493).
                "auth_json_present": Path("/home/agastya/.codex/auth.json").exists(),
                "codex_usable": _codex_usable(state),
                "last_selftest_ok": state.last_selftest_ok,
                "last_selftest_at": state.last_selftest_at,
                "sandbox_mode": state.sandbox_mode,
            },
            "claude": {
                "key_present": bool(executor.anthropic_api_key()),
                "claude_usable": _claude_usable(state),
                "last_selftest_ok": state.claude_last_selftest_ok,
                "last_selftest_at": state.claude_last_selftest_at,
                "claude_cost_usd": claude_cost,
            },
            "limits": {
                "active_request": "locked" if bridge.lock.locked() else None,
                "downloads_today": downloads_today,
                "attempts_today": attempts_by_strategy,
                "daily_cap": limits.DAILY_CAP,
                "min_spacing_seconds": limits.MIN_SPACING_SECONDS,
            },
            "token_configured": token_configured,
            "version": _paperika_version(),
        }
        return JSONResponse(status_code=http_status, content=body)

    # --- /selftest/codex ---

    @app.post("/selftest/codex")
    async def selftest_codex(request: Request) -> Response:
        denied = require_token(request)
        if denied is not None:
            return denied
        if bridge.lock.locked():
            return _json_error(429, "busy", "a download or selftest is in progress")
        async with bridge.lock:
            run_dir = bridge.config.db_path.parent / "codex_runs" / "selftest"
            result = await executor.run_selftest(run_dir)
            now_iso = _utc_now().isoformat()
            if result.ok:
                bridge.state_store.update(
                    sandbox_mode=result.sandbox_mode,
                    last_selftest_ok=True,
                    last_selftest_at=now_iso,
                    last_variant1_failure_reason=result.variant1_failure_reason,
                )
                return JSONResponse(
                    status_code=200,
                    content={"ok": True, "wall_seconds": round(result.wall_seconds, 2),
                             "sandbox_mode": result.sandbox_mode},
                )
            bridge.state_store.update(last_selftest_ok=False, last_selftest_at=now_iso)
            if result.error_code == "codex_auth":
                return _json_error(503, "codex_auth", result.detail, ok=False)
            return _json_error(502, "executor_failed", result.detail, ok=False)

    # --- /selftest/claude (AGA-1460) ---

    @app.post("/selftest/claude")
    async def selftest_claude(request: Request) -> Response:
        denied = require_token(request)
        if denied is not None:
            return denied
        api_key = executor.anthropic_api_key()
        if not api_key:
            return _json_error(
                503, "claude_auth", "ANTHROPIC_API_KEY is not available to the bridge", ok=False
            )
        if bridge.lock.locked():
            return _json_error(429, "busy", "a download or selftest is in progress")
        async with bridge.lock:
            run_dir = bridge.config.db_path.parent / "codex_runs" / "selftest_claude"
            result = await executor.run_claude_selftest(run_dir, api_key)
            now_iso = _utc_now().isoformat()
            bridge.state_store.update(
                claude_last_selftest_ok=result.ok, claude_last_selftest_at=now_iso
            )
            if result.ok:
                return JSONResponse(
                    status_code=200,
                    content={"ok": True, "wall_seconds": round(result.wall_seconds, 2)},
                )
            if result.error_code == "claude_auth":
                return _json_error(503, "claude_auth", result.detail, ok=False)
            return _json_error(502, "executor_failed", result.detail, ok=False)

    # --- /requests/{id} ---

    @app.get("/requests/{request_id}")
    async def get_request(request_id: int, request: Request) -> Response:
        denied = require_token(request)
        if denied is not None:
            return denied
        req = bridge.db.get_request(request_id)
        if req is None:
            return _json_error(404, "not_found", f"request {request_id} unknown")
        with bridge.db.transaction() as conn:
            conn.row_factory = __import__("sqlite3").Row
            attempts = conn.execute(
                "SELECT * FROM paper_attempts WHERE request_id = ? ORDER BY id ASC", (request_id,)
            ).fetchall()
        return JSONResponse(
            status_code=200,
            content={
                "request": dict(req),
                "attempts": [dict(a) for a in attempts],
                # DERIVED, not stored (review finding 2, 2026-07-31): a count of
                # this request's paper_attempts rows (tier attempts). Deliberately
                # NOT written into paper_requests.attempt_count — that column is
                # downloader.py's whole-request retry counter, and writing a
                # per-tier count into it corrupted that path's backoff/retry
                # budget. See the column comment in db.py's SCHEMA_SQL.
                "tier_attempt_count": len(attempts),
            },
        )

    # --- /requests/{id}/screenshot (AGA-596 P3) ---

    @app.get("/requests/{request_id}/screenshot")
    async def get_request_screenshot(request_id: int, request: Request) -> Response:
        """Serve the request's stored manual-handoff screenshot (falling back to
        the latest attempt's screenshot) as image bytes. The bridge is docker0-only
        (no shell access needed by KC/an operator) — this is what KC's own
        screenshot proxy fetches from."""
        denied = require_token(request)
        if denied is not None:
            return denied
        req = bridge.db.get_request(request_id)
        if req is None:
            return _json_error(404, "not_found", f"request {request_id} unknown")
        raw_path = req["manual_screenshot_path"] or bridge.latest_attempt_screenshot(request_id)
        if not raw_path:
            return _json_error(404, "not_found", "no screenshot recorded for this request")
        path = bridge.resolve_screenshot_path(raw_path)
        if path is None:
            return _json_error(404, "not_found", "screenshot file is missing or outside the allowed area")
        media_type = _SCREENSHOT_MEDIA_TYPES.get(path.suffix.lower(), "application/octet-stream")
        return Response(content=path.read_bytes(), media_type=media_type)

    # --- /download ---

    @app.post("/download")
    async def download(payload: DownloadRequest, request: Request) -> Response:
        denied = require_token(request)
        if denied is not None:
            return denied

        # 1. validate + normalize (boundary sanitization, §2.4)
        doi = bridge.normalize_doi(payload.doi)
        if doi is None:
            return _json_error(422, "invalid_doi", "doi must match ^10\\.\\d{4,9}/\\S+$")
        title = bridge.sanitize_title(payload.title)
        url_ok, source_url = bridge.sanitize_source_url(payload.source_url)
        if not url_ok:
            return _json_error(422, "invalid_source_url", "source_url must be an http(s) URL")
        start_url = source_url or f"https://doi.org/{doi}"

        # 2. dedupe (cache hit) — zero publisher traffic
        cached = bridge.dedupe_hit(doi=doi, title=title)
        if cached is not None:
            data = cached.read_bytes()
            return Response(
                content=data, media_type="application/pdf",
                headers={
                    "X-Paperika-Sha256": bridge._sha256(cached),
                    "X-Paperika-Verified": "doi",
                    "X-Paperika-Cached": "true",
                },
            )

        # 3. serialization lock (non-blocking)
        if bridge.lock.locked():
            return _json_error(429, "busy", "a download is in progress")
        async with bridge.lock:
            try:
                return await _download_locked(bridge, doi=doi, title=title, start_url=start_url,
                                              requested_by=payload.requested_by)
            except Exception as exc:  # CancelledError is a BaseException: not caught
                logger.exception("unhandled exception in /download ladder (doi=%s)", doi)
                return _json_error(
                    _TAXONOMY["bridge_error"][2], _TAXONOMY["bridge_error"][3],
                    f"{type(exc).__name__}: {exc}"[:300],
                )

    return app


async def _download_locked(
    bridge: Bridge, *, doi: str, title: str, start_url: str, requested_by: str
) -> Response:
    """The tiered download ladder (AGA-339). Deterministic tier 1 (direct HTTP
    fetch) and tier 2 (scripted Playwright) run BEFORE the tier-3 codex executor,
    each rate-disciplined on its OWN strategy. Tiers 1/2 only ever succeed or
    escalate — codex stays the sole terminal-classification authority, except a
    both-tiers bot wall with codex unavailable, mapped to a retryable 502
    bot_wall_unclassified (AGA-596 P1)."""
    state = bridge.state_store.load()
    deadline = MONOTONIC() + REQUEST_BUDGET_SECONDS

    # 4. rate pre-flight across all three strategies. A tier whose strategy is
    # rate-blocked is silently skipped (no attempt row). Only when EVERY tier is
    # blocked do we 429 — with the smallest retry_after among them.
    claude_ready = _claude_ready(state)
    rate_strats = ("direct_fetch", "scripted_browser", "codex_bridge") + (
        ("claude_bridge",) if claude_ready else ()
    )
    with bridge.db.transaction() as conn:
        rate = {strat: limits.check_rate(conn, strat) for strat in rate_strats}
    if all(not d.allowed for d in rate.values()):
        retry = [d.retry_after_seconds for d in rate.values() if d.retry_after_seconds is not None]
        extra = {"retry_after_seconds": min(retry)} if retry else {}
        return _json_error(429, "rate_limited", "all download tiers are rate-limited", **extra)

    # AGA-1256: tier 1 merely cooling down is not a reason to skip it. Skipping
    # escalated IEEE papers straight to the scripted/codex tiers, and with codex
    # unusable the answer was selftest_required for a paper tier 1 fetches in
    # seconds (2026-10-13: every second back-to-back request). Wait out a short
    # cooldown instead (the lock is held either way; the client budget is 1200s).
    tier1 = rate["direct_fetch"]
    if (
        not tier1.allowed
        and tier1.error_code == "cooldown"
        and tier1.retry_after_seconds is not None
        and tier1.retry_after_seconds <= TIER1_COOLDOWN_WAIT_MAX
    ):
        await SLEEP(tier1.retry_after_seconds)
        with bridge.db.transaction() as conn:
            rate["direct_fetch"] = limits.check_rate(conn, "direct_fetch")

    # 5. chrome pre-flight. A sick Chrome no longer fails the whole request: tier 1
    # runs cookie-less regardless, tier 2 is skipped, and tier 3 keeps the legacy
    # 503 chrome_down behavior.
    await chrome.idle_recycle_if_stale()
    probe = await chrome.probe(fresh=True)
    chrome_ok = probe.ok
    if not chrome_ok:
        await chrome.recover()
        probe = await chrome.probe(fresh=True)
        chrome_ok = probe.ok

    # 6. write-ahead bookkeeping is lazy — the FIRST tier that actually runs
    # materializes the request + run_dir, so a fully-gated request (no eligible
    # tier) leaves no dangling in_progress row.
    created: dict[str, Any] = {}

    def ensure_request() -> tuple[int, Path]:
        if "request_id" not in created:
            request_id = bridge.db.create_request(
                raw_input=doi, inferred_title=title or None, inferred_doi=doi,
                inferred_url=start_url, paper_id=None,
            )
            bridge.db.update_request_status(request_id, "in_progress")
            run_dir = bridge.config.db_path.parent / "codex_runs" / str(request_id)
            run_dir.mkdir(parents=True, exist_ok=True)
            created["request_id"] = request_id
            created["run_dir"] = run_dir
        return created["request_id"], created["run_dir"]

    def finalize_if_created(response: Response) -> Response:
        """Mark a materialized-but-unfinished request 'failed' before a gated
        codex-unavailable return (review fix): a deterministic tier that ran set
        the request in_progress, and if codex is then unavailable these early
        returns finalize nothing themselves — the request would otherwise dangle
        in_progress until the NEXT bridge restart's startup_sweep (a backstop for
        a crash, not the normal path here; this finalizes it synchronously, right
        now). No request materialized ⇒ nothing to finalize.

        If a SINGLE deterministic tier walled (both_wall was false — the OTHER
        tier missed/skipped/errored, so this isn't the both-tiers-wall path) and
        codex is merely unavailable (selftest_required/chrome_down/cooldown/
        daily_cap), that wall evidence is durable in paper_attempts and must not
        be silently discarded either (review round-3 finding 3): build and
        persist the same ManualIntervention handoff a live wall finish would,
        and emit the same notification. The response's own HTTP status/
        error_code is unchanged either way — only the persisted row and the
        notification gain the handoff."""
        request_id = created.get("request_id")
        if request_id is not None:
            walled_tiers, last_wall_url, last_screenshot = bridge.wall_evidence(request_id)
            manual = None
            if walled_tiers:
                current_url = _preferred_current_url(bridge, request_id, last_wall_url)
                manual = ManualIntervention(
                    reason=(
                        f"{' and '.join(walled_tiers)} hit a bot/CAPTCHA wall; codex was "
                        "unavailable to classify further"
                    ),
                    screenshot_path=last_screenshot,
                    page_title=None,
                    current_url=current_url,
                    suggested_next_action=_wall_suggested_action(current_url),
                )
            try:
                applied = bridge.db.update_request_status(request_id, "failed", manual=manual)
            except KeyError:
                applied = False
            if applied and manual is not None:
                bridge.emit(
                    event_type="bot_wall", request_id=request_id, paper_id=None,
                    status_after="failed", status_before="in_progress",
                    message=f"{' and '.join(walled_tiers)} hit a bot/CAPTCHA wall; codex unavailable",
                )
        return response

    tier_kinds: dict[str, str] = {}

    # --- tier 1: direct HTTP fetch (no Chrome-health requirement) ---
    if rate["direct_fetch"].allowed:
        request_id, run_dir = ensure_request()
        outcome = await _run_deterministic_tier(
            bridge, strategy="direct_fetch", fetch_fn=attempt_direct_fetch,
            budget=TIER1_BUDGET, request_id=request_id, run_dir=run_dir,
            doi=doi, title=title, start_url=start_url,
        )
        if isinstance(outcome, Response):
            return outcome
        tier_kinds["direct_fetch"] = outcome

    # --- tier 2: scripted Playwright (needs a healthy Chrome) ---
    if rate["scripted_browser"].allowed and chrome_ok:
        request_id, run_dir = ensure_request()
        outcome = await _run_deterministic_tier(
            bridge, strategy="scripted_browser", fetch_fn=attempt_scripted_fetch,
            budget=TIER2_BUDGET, request_id=request_id, run_dir=run_dir,
            doi=doi, title=title, start_url=start_url,
        )
        if isinstance(outcome, Response):
            return outcome
        tier_kinds["scripted_browser"] = outcome

    # --- tier 3: codex (the terminal classification authority) ---
    both_wall = (
        tier_kinds.get("direct_fetch") == "wall"
        and tier_kinds.get("scripted_browser") == "wall"
    )
    codex_ready = rate["codex_bridge"].allowed and bool(state.sandbox_mode) and chrome_ok
    claude_go = claude_ready and rate["claude_bridge"].allowed and chrome_ok
    if codex_ready or claude_go:
        request_id, run_dir = ensure_request()
        return await _run_tier3(
            bridge, request_id=request_id, run_dir=run_dir, doi=doi, title=title,
            start_url=start_url, sandbox_mode=state.sandbox_mode if codex_ready else None,
            claude_go=claude_go, deadline=deadline,
        )

    # Codex is unavailable. Both deterministic tiers hitting a wall is the one
    # signal strong enough to terminate without codex — map it to a retryable 502
    # bot_wall_unclassified instead of selftest_required (AGA-596 P1).
    if both_wall:
        return _finish_wall_evidence(bridge, request_id=created["request_id"])
    if not state.sandbox_mode:
        return finalize_if_created(
            _json_error(503, "selftest_required", "run POST /selftest/codex first")
        )
    if not chrome_ok:
        return finalize_if_created(_json_error(503, "chrome_down", "Chrome CDP unreachable"))
    # codex sandbox + chrome are fine but its strategy is rate-blocked.
    decision = rate["codex_bridge"]
    if decision.error_code == "cooldown":
        return finalize_if_created(
            _json_error(429, "cooldown", "minimum spacing not elapsed",
                        retry_after_seconds=decision.retry_after_seconds)
        )
    return finalize_if_created(_json_error(429, "daily_cap", "daily download cap reached"))


def _locate_downloaded(
    bridge: Bridge, file_path: str | None, window_start: datetime, window_end: datetime
) -> Path | None:
    """Resolve a reported download to a trusted on-disk path (shared by codex and
    the deterministic tiers): accept the reported file ONLY when it passes the
    containment + mtime-window gate, else fall back to the window locator."""
    if file_path:
        p = Path(file_path)
        if bridge._within_download_window(p, window_start, window_end):
            return p
    return bridge._locate_in_window(window_start, window_end)


async def _run_deterministic_tier(
    bridge: Bridge, *, strategy: str, fetch_fn: Any, budget: float,
    request_id: int, run_dir: Path, doi: str, title: str, start_url: str,
) -> Response | str:
    """Run one deterministic tier (1 or 2). Writes a strategy-tagged write-ahead
    attempt row, runs the fetcher (which never raises and only lands bytes into
    download_dir), then either accepts via the SAME verify path codex uses (returns
    a Response) or resolves the attempt as a non-terminal miss and returns the
    effective miss kind for the caller to escalate on. The fetcher's final_url is
    persisted onto the attempt row itself (``current_url``, review finding 1) —
    durable evidence a later finish can read back via ``bridge.wall_evidence``
    even across a bridge restart, rather than an in-memory value."""
    attempt_id = bridge.write_ahead(
        request_id=request_id, paper_id=None, run_dir=str(run_dir), strategy=strategy
    )
    started = _utc_now()
    result = await fetch_fn(
        doi=doi, title=title, start_url=start_url,
        download_dir=bridge.config.download_dir, cdp_http_url=chrome.CDP_HTTP,
        budget_seconds=budget,
    )
    ended = _utc_now()

    if result.kind == "downloaded":
        located = _locate_downloaded(bridge, result.file_path, started, ended)
        verify = await _verify_and_finish(
            bridge, located=located, doi=doi, title=title, request_id=request_id,
            attempt_id=attempt_id, final_url=result.final_url, screenshot=None,
            salvaged=False, run_dir=run_dir, tier_label=strategy,
        )
        if isinstance(verify, Response):
            return verify
        # A deterministic URL rule can grab the wrong object; codex stays the
        # wrong_paper authority, so a tier verify-fail is quarantined (inside
        # _verify_and_finish) but ESCALATES rather than terminating.
        outcome = "verify_failed" if verify == "verify_failed" else "no_pdf"
        bridge.resolve_attempt(attempt_id, outcome=outcome, message=result.notes or verify,
                               current_url=result.final_url)
        return outcome

    bridge.resolve_attempt(attempt_id, outcome=result.kind, message=result.notes or result.kind,
                           current_url=result.final_url)
    return result.kind


# Executor failures that never reached a verdict on the paper: the only ones the
# claude fallback may take over from codex. A codex VERDICT (downloaded, bot_wall,
# paywalled_no_access, gave_up, wrong_paper, throttled) is never second-guessed.
_FALLBACK_KINDS = {"executor_died", "timeout", "auth_error", "codex_quota"}


async def _run_tier3(
    bridge: Bridge, *, request_id: int, run_dir: Path, doi: str, title: str,
    start_url: str, sandbox_mode: str | None, claude_go: bool, deadline: float,
) -> Response:
    """Tier 3 driver chain (AGA-1460): codex first when usable, claude as the
    automatic fallback. ``sandbox_mode=None`` means codex is unusable (no selftest /
    rate-blocked / chrome) and claude runs directly."""
    if sandbox_mode is not None:
        response = await _run_executor_attempt(
            bridge, driver="codex", request_id=request_id, run_dir=run_dir, doi=doi,
            title=title, start_url=start_url, sandbox_mode=sandbox_mode,
            # Decided AFTER codex ran: only fall back if the claude run still fits
            # inside the request deadline (else codex's retryable failure stands).
            defer_failure=claude_go,
            can_fallback=lambda: deadline - MONOTONIC() >= FALLBACK_MIN_SECONDS,
        )
        if response is not None:
            return response
    # codex died / is unusable and claude is ready: its own attempt row.
    api_key = executor.anthropic_api_key()
    assert api_key  # claude_go implies a key
    response = await _run_executor_attempt(
        bridge, driver="claude", request_id=request_id, run_dir=run_dir, doi=doi,
        title=title, start_url=start_url, sandbox_mode=None, defer_failure=False,
        api_key=api_key,
        claude_wall=max(1.0, min(float(executor.EXEC_WALL_SECONDS),
                                 deadline - MONOTONIC() - FALLBACK_TAIL_SECONDS)),
    )
    assert response is not None
    return response


async def _run_executor_attempt(
    bridge: Bridge, *, driver: str, request_id: int, run_dir: Path, doi: str, title: str,
    start_url: str, sandbox_mode: str | None, defer_failure: bool, api_key: str | None = None,
    can_fallback: Any = None, claude_wall: float = float(executor.EXEC_WALL_SECONDS),
) -> Response | None:
    """One tier-3 executor attempt (``driver`` = codex | claude). Writes its own
    write-ahead row, runs the executor, resolves the terminal taxonomy (§2.7) — the
    only tier that produces terminal wrong_paper / throttled / paywalled / bot_wall /
    gave_up classifications. With ``defer_failure`` a fallback-eligible failure
    (executor_died/timeout/auth/quota; salvage already tried) resolves ONLY the
    attempt row and returns None so the caller can try the next driver."""
    strategy = f"{driver}_bridge"
    attempt_id = bridge.write_ahead(
        request_id=request_id, paper_id=None, run_dir=str(run_dir), strategy=strategy
    )
    executor.write_task_json(run_dir, doi=doi, title=title, start_url=start_url)
    prompt = executor.PROMPT_TEMPLATE.format(run_dir=str(run_dir))
    started = _utc_now()
    if driver == "claude":
        exec_result = await run_claude(run_dir, prompt, api_key or "", wall_seconds=claude_wall)
        # Belt and braces (run_claude already redacts): the key never reaches a DB
        # row, notification or HTTP error detail.
        exec_result.notes = executor.redact_secret(exec_result.notes, api_key)
    else:
        exec_result = await run_codex(run_dir, prompt, sandbox_mode or "")
    ended = _utc_now()
    screenshot = str(run_dir / "final.png") if (run_dir / "final.png").exists() else None
    if exec_result.cost_usd is not None:
        with bridge.db.transaction() as conn:
            limits.record_attempt_cost(conn, attempt_id, exec_result.cost_usd)

    bad_outcome = exec_result.kind in {"timeout", "gave_up", "executor_died", "claude_budget"}
    if defer_failure and can_fallback is not None and not can_fallback():
        defer_failure = False  # no time left for a fallback: codex's failure stands
    response = await _resolve_outcome(
        bridge, exec_result=exec_result, request_id=request_id, attempt_id=attempt_id,
        doi=doi, title=title, run_dir=run_dir, window_start=started, window_end=ended,
        screenshot=screenshot, tier_label=strategy,
        defer_failure=defer_failure,
    )

    # A runtime auth failure means that driver's login/key is dead. Retrying burns
    # the wall on every request while it stays dead. Clear its readiness latch so
    # the ladder stops attempting it (codex: surfaces selftest_required; claude:
    # _claude_ready goes false) until an operator fixes it and re-runs the matching
    # /selftest (AGA-493, AGA-1460). Done BEFORE chrome recovery: that awaits up to
    # ~30s, and a CancelledError mid-recovery must not leave a known-broken driver
    # latched ready.
    if exec_result.kind == "auth_error":
        bridge.state_store.update(sandbox_mode=None, last_selftest_ok=False)
    elif exec_result.kind == "claude_auth":
        bridge.state_store.update(claude_last_selftest_ok=False)

    # chrome recovery on bad outcomes (still holding the lock)
    if bad_outcome or exec_result.kind in {"auth_error", "claude_auth"}:
        rec = await chrome.recover()
        bridge.state_store.update(last_recovery_action=rec.action)

    return response


def _preferred_current_url(bridge: Bridge, request_id: int, *candidates: str | None) -> str | None:
    """First non-None of ``candidates`` (already in caller-supplied preference
    order — e.g. codex's own final_url, then the last deterministic tier's wall
    URL), else the request's own stored ``inferred_url`` (the DOI/source URL the
    ladder started from). For every request the bridge itself creates,
    ``ensure_request()`` always passes a computed, non-None ``start_url`` here —
    but ``Database.create_request()`` itself accepts ``inferred_url=None`` (used
    by callers outside the bridge, e.g. downloader.py's freeform-query enqueue
    path), so this is NOT a universal invariant of the column (review round-3
    finding 5 — the previous docstring overclaimed it always holds a value).
    This function tolerates that: with no non-None candidate and no
    inferred_url, it returns None — never a crash. Never leave a
    ManualIntervention's current_url NULL when the request row DOES hold a
    usable URL though (AGA-596 review round 2, finding 3) — codex's
    OUTCOME_SCHEMA explicitly permits a null final_url."""
    for candidate in candidates:
        if candidate:
            return candidate
    req = bridge.db.get_request(request_id)
    return req["inferred_url"] if req else None


def _wall_suggested_action(current_url: str | None) -> str:
    """Operator-facing next step for a human-solvable bot/CAPTCHA wall (AGA-596
    L1) — the bridge cannot solve a CAPTCHA itself, so hand the operator the
    concrete steps instead of a bare error code."""
    target = current_url or "the publisher page for this DOI"
    return (
        f"Open {target} in a browser, solve the CAPTCHA/verification if shown, "
        "download the PDF yourself, then add it to Knowledge Center via the inbox "
        "upload (Upgrade PDF) instead of retrying automatically."
    )


def _finish_wall_evidence(bridge: Bridge, *, request_id: int) -> Response:
    """Both deterministic tiers hit a bot wall and codex is unavailable to classify.
    Return a retryable 502 bot_wall_unclassified (honest, retryable, and distinct
    from codex-confirmed bot_wall — AGA-596 P1) rather than selftest_required — the
    tier attempt rows already recorded the two walls. Also populate the request's
    ManualIntervention (AGA-596 P2): neither deterministic tier captures a
    screenshot or page title (not in either fetcher's result schema), so those
    stay None. current_url comes from ``bridge.wall_evidence`` (durable — read
    back from paper_attempts, not an in-memory value from this call)."""
    _, event_type, http_status, error_code = _TAXONOMY["bot_wall_unclassified"]
    message = "direct_fetch and scripted_browser both hit a bot wall; codex unavailable to classify"
    # codex never ran in this path (codex_ready was false), so its 'bot_wall'
    # outcome can't appear here — the screenshot is always None regardless.
    walled_tiers, last_wall_url, _last_screenshot = bridge.wall_evidence(request_id)
    tiers_desc = " and ".join(walled_tiers) if walled_tiers else "the deterministic tiers"
    current_url = _preferred_current_url(bridge, request_id, last_wall_url)
    manual = ManualIntervention(
        reason=f"{tiers_desc} hit a bot/CAPTCHA wall; no classifier available (codex unavailable)",
        screenshot_path=None,
        page_title=None,
        current_url=current_url,
        suggested_next_action=_wall_suggested_action(current_url),
    )
    try:
        applied = bridge.db.update_request_status(request_id, "failed", manual=manual)
    except KeyError:
        applied = False
    # A guarded no-op (review round-2 finding 2) means the request had somehow
    # already reached a success status — the persisted row did NOT change to
    # "failed", so don't emit a notification claiming it did. The HTTP response
    # below still accurately describes what THIS attempt did.
    if applied:
        bridge.emit(
            event_type=event_type, request_id=request_id, paper_id=None,
            status_after="failed", status_before="in_progress", message=message,
        )
    return _json_error(http_status, error_code, message, request_id=str(request_id))


async def _resolve_outcome(
    bridge: Bridge, *, exec_result: executor.ExecResult, request_id: int, attempt_id: int,
    doi: str, title: str, run_dir: Path, window_start: datetime, window_end: datetime,
    screenshot: str | None, tier_label: str = "codex_bridge", defer_failure: bool = False,
) -> Response | None:
    """Resolve an executor result. ``defer_failure`` (tier-3 fallback, AGA-1460):
    a failure that survives salvage resolves only the attempt row and returns None,
    leaving the request open for the next driver."""
    kind = exec_result.kind

    def _fail(*, kind: str, message: str) -> Response | None:
        if defer_failure and kind in _FALLBACK_KINDS:
            bridge.resolve_attempt(
                attempt_id, outcome=_TAXONOMY[kind][0], message=message,
                screenshot_path=screenshot, current_url=exec_result.final_url,
            )
            return None
        return _finish_failure(bridge, kind=kind, request_id=request_id,
                               attempt_id=attempt_id, screenshot=screenshot,
                               message=message, final_url=exec_result.final_url)

    # downloaded path
    if kind == "downloaded":
        # The codex-REPORTED file_path is trusted only after containment + window
        # gates (review fix, finding 1): it must resolve INSIDE config.download_dir
        # and have an mtime within the run window. Anything else (out-of-tree,
        # stale, or a steered injection naming an arbitrary file) is discarded —
        # never used, never quarantined — and we fall back to the window locator.
        located = None
        if exec_result.file_path:
            p = Path(exec_result.file_path)
            if bridge._within_download_window(p, window_start, window_end):
                located = p
        if located is None:
            located = bridge._locate_in_window(window_start, window_end)
        verify = await _verify_and_finish(
            bridge, located=located, doi=doi, title=title, request_id=request_id,
            attempt_id=attempt_id, final_url=exec_result.final_url, screenshot=screenshot,
            salvaged=False, run_dir=run_dir, tier_label=tier_label,
        )
        if isinstance(verify, Response):
            return verify
        if verify == "verify_failed":
            # A real, in-tree, in-window PDF was found but its identity did not
            # match ⇒ wrong_paper (terminal). Only reachable when verification
            # actually ran and failed.
            return _finish_failure(bridge, kind="wrong_paper", request_id=request_id,
                                   attempt_id=attempt_id, screenshot=screenshot,
                                   message="downloaded PDF failed identity verification",
                                   final_url=exec_result.final_url)
        # No locatable candidate at all (the executor MISREPORTED 'downloaded' —
        # nothing was ever verified). A claim with no file is a death, not a
        # verdict: retryable executor_died, never terminal gave_up / wrong_paper
        # (review fix 4a; AGA-490 reclassified it from gave_up).
        return _fail(kind="executor_died", message="claimed download not found")

    # salvage pass on timeout/gave_up/executor_died — a run that died (or timed out)
    # may still have written the PDF before dying, so it gets the same locate+verify
    # pass rather than being failed blind.
    if kind in {"timeout", "gave_up", "executor_died", "claude_budget"}:
        located = bridge._locate_in_window(window_start, window_end)
        if located is not None and bridge._mechanical_gates(located):
            ok, reason = bridge.verify_identity(doi=doi, title=title, path=located)
            if ok:
                salvage = await _verify_and_finish(
                    bridge, located=located, doi=doi, title=title, request_id=request_id,
                    attempt_id=attempt_id, final_url=exec_result.final_url, screenshot=screenshot,
                    salvaged=True, run_dir=run_dir, tier_label=tier_label,
                )
                if isinstance(salvage, Response):
                    return salvage
            else:
                # Salvage candidate failed identity verify ⇒ quarantine it but KEEP
                # the original failure outcome (design §2.6 step 4; review fix,
                # finding 2). Otherwise the orphan sits in ~/Downloads/papers; with
                # 558 pre-existing files the mtime window is the only guard.
                _quarantine(located, run_dir=run_dir,
                            reason=f"salvage candidate failed verification: {reason}")
        # salvage found nothing valid ⇒ original failure taxonomy
        return _fail(kind=kind, message=exec_result.notes or kind)

    if defer_failure and kind in _FALLBACK_KINDS:  # auth_error / codex_quota: nothing to salvage
        return _fail(kind=kind, message=exec_result.notes or kind)

    # terminal failures (throttled/paywalled/bot_wall/gave_up-with-no-file/auth).
    # _finish_failure reads deterministic-tier wall evidence itself (bridge.
    # wall_evidence) and populates ManualIntervention whenever it exists, not only
    # when kind == "bot_wall" (review finding 1, 2026-07-31).
    return _finish_failure(bridge, kind=kind, request_id=request_id, attempt_id=attempt_id,
                           screenshot=screenshot, message=exec_result.notes or kind,
                           final_url=exec_result.final_url)


def _quarantine(path: Path, *, run_dir: Path, reason: str) -> None:
    """Move a verify-failed candidate out of the dedupe dir into the run's
    quarantine/ — never return unverified bytes, never let it bait dedupe."""
    try:
        qdir = run_dir / "quarantine"
        qdir.mkdir(parents=True, exist_ok=True)
        target = qdir / path.name
        path.replace(target)
        (qdir / (path.name + ".reason.txt")).write_text(reason, encoding="utf-8")
    except OSError:
        pass


async def _verify_and_finish(
    bridge: Bridge, *, located: Path | None, doi: str, title: str, request_id: int,
    attempt_id: int, final_url: str | None, screenshot: str | None, salvaged: bool,
    run_dir: Path, tier_label: str,
) -> Response | str:
    """Run mechanical + identity gates; on pass, link + resolve + stream bytes.
    Returns the PDF Response on success, or a string signal the caller maps to a
    taxonomy outcome: ``"no_candidate"`` (no valid file located — nothing verified)
    vs ``"verify_failed"`` (a candidate was found and quarantined after failing
    identity verification)."""
    if located is None or not bridge._mechanical_gates(located):
        return "no_candidate"
    ok, reason = bridge.verify_identity(doi=doi, title=title, path=located)
    if not ok:
        # Quarantine the candidate (downloaded path ⇒ wrong_paper; salvage ⇒ keep
        # the original failure). Either way move it out of the dedupe dir. The
        # caller resolves the attempt outcome (no double-resolve here).
        _quarantine(located, run_dir=Path(run_dir), reason=reason)
        return "verify_failed"

    sha = bridge._sha256(located)
    paper_id = bridge.link_paper(doi=doi, title=title, path=located, final_url=final_url)
    note = "salvaged" if salvaged else "downloaded"
    bridge.resolve_attempt(attempt_id, outcome="completed", paper_id=paper_id,
                           message=note, screenshot_path=screenshot)
    bridge.db.update_request_status(request_id, "completed", paper_id=paper_id)
    bridge.emit(
        event_type="paper_downloaded", request_id=request_id, paper_id=paper_id,
        status_after="completed", status_before="in_progress",
        message=f"{note} via paperika bridge",
    )
    data = located.read_bytes()
    headers = {
        "X-Paperika-Request-Id": str(request_id),
        "X-Paperika-Sha256": sha,
        "X-Paperika-Verified": "doi" if doi else "title",
        "X-Paperika-Tier": tier_label,
    }
    if salvaged:
        headers["X-Paperika-Salvaged"] = "true"
    return Response(content=data, media_type="application/pdf", headers=headers)


def _codex_wall_reason(walled_tiers: list[str], kind: str) -> str:
    """Operator-facing reason for a bot-wall ManualIntervention originating from
    (or alongside) the codex tier — review finding 1 (2026-07-31): populated
    whenever deterministic-tier wall evidence exists, not only when codex itself
    confirms bot_wall, so a post-escalation codex crash/timeout/auth failure
    cannot discard proven wall evidence."""
    if not walled_tiers:
        return "codex classified this request as a bot/CAPTCHA wall"
    tiers = " and ".join(walled_tiers)
    if kind == "bot_wall":
        return f"{tiers} hit a bot/CAPTCHA wall before escalating; codex confirmed bot_wall"
    return (
        f"{tiers} hit a bot/CAPTCHA wall before escalating; codex's own result was "
        f"'{kind}' (not a confirmed wall, but the tier evidence still stands)"
    )


def _finish_failure(
    bridge: Bridge, *, kind: str, request_id: int, attempt_id: int,
    screenshot: str | None, message: str, final_url: str | None = None,
) -> Response:
    outcome, event_type, http_status, error_code = _TAXONOMY[kind]
    bridge.resolve_attempt(attempt_id, outcome=outcome, message=message, screenshot_path=screenshot,
                           current_url=final_url)
    # Deterministic-tier (or a PRIOR codex run's) wall evidence is read back from
    # paper_attempts (durable — review finding 1), not threaded in-memory through
    # the call chain. exclude_attempt_id=attempt_id: the resolve_attempt call just
    # above may itself have written outcome='bot_wall' for THIS attempt (when
    # kind == "bot_wall") — exclude it so a codex-confirmed wall doesn't
    # self-report as its own prior evidence (review round-3 finding 2).
    walled_tiers, last_wall_url, _last_screenshot = bridge.wall_evidence(
        request_id, exclude_attempt_id=attempt_id
    )
    manual = None
    if kind == "bot_wall" or walled_tiers:
        # Preference order for current_url (AGA-596 review round 2, finding 3):
        # codex's own final_url, else the last deterministic tier's wall URL,
        # else the request's own stored inferred_url — codex's OUTCOME_SCHEMA
        # permits a null final_url, and a codex-only wall with no prior
        # deterministic wall must still get a usable URL, not a bare NULL.
        current_url = _preferred_current_url(bridge, request_id, final_url, last_wall_url)
        manual = ManualIntervention(
            reason=_codex_wall_reason(walled_tiers, kind),
            screenshot_path=screenshot,
            page_title=None,
            current_url=current_url,
            suggested_next_action=_wall_suggested_action(current_url),
        )
    try:
        applied = bridge.db.update_request_status(request_id, "failed", manual=manual)
    except KeyError:
        applied = False
    # Skip the notification on a guarded no-op (review round-2 finding 2) — the
    # persisted row did not actually change to "failed" in that case.
    if applied:
        bridge.emit(
            event_type=event_type, request_id=request_id, paper_id=None,
            status_after="failed", status_before="in_progress", message=message,
        )
    return _json_error(http_status, error_code, message, request_id=str(request_id),
                       screenshot=screenshot)


# Module-level app for uvicorn (paperika.bridge.app:app). Built lazily so importing
# this module for unit tests (with an injected temp config) does not touch the real
# papers.db. uvicorn resolves the `app` attribute, which triggers create_app() once.
_app_singleton: FastAPI | None = None


def __getattr__(name: str) -> Any:
    global _app_singleton
    if name == "app":
        if _app_singleton is None:
            _app_singleton = create_app()
        return _app_singleton
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
