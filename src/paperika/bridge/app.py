"""paperika institutional-download bridge — FastAPI app (§2).

Manual/on-demand only: the bridge acts only on inbound HTTP, never polls. A
single in-process asyncio.Lock serializes /download and /selftest/codex; the unit
runs --workers 1 so the lock is global. Rate discipline + the write-ahead attempt
record live in limits.py; Chrome health in chrome.py; the codex executor in
executor.py.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import functools
import hashlib
import hmac
import json
import logging
import os
from pathlib import Path
import re
import secrets
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
from . import chrome, elsevier_api, executor, limits, tcd_browser
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
# FALLBACK_MIN_SECONDS remain (re-checked right before its attempt row, AFTER codex
# salvage + Chrome recovery), with its wall capped at remaining - CLEANUP_RESERVE_SECONDS.
REQUEST_BUDGET_SECONDS = 1140
FALLBACK_MIN_SECONDS = 180
TIER3_SKIP_RETRY_SECONDS = 30
# Time held back at the end of the budget for everything that runs AFTER a claude
# attempt's wall: salvage drain (10) + PDF verification (~20) + Chrome recovery
# (~70) + margin. The claude wall is capped to remaining - this.
CLEANUP_RESERVE_SECONDS = 120
# AGA-1461: when the claude fallback is ready at tier-3 start, codex's wall is capped
# so a full codex TIMEOUT still leaves claude a guaranteed window:
#   codex_wall = min(EXEC_WALL, remaining - R - (CLAUDE_FALLBACK_WINDOW + R))
# (R = CLEANUP_RESERVE_SECONDS: one for codex's own post-run cleanup, one for
# claude's). The post-codex re-check demands the same window: claude's wall
# (remaining - R) must be >= CLAUDE_FALLBACK_WINDOW_SECONDS. A claude-only or
# codex-only tier 3 keeps FALLBACK_MIN_SECONDS as its minimum useful wall.
CLAUDE_FALLBACK_WINDOW_SECONDS = 300
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
# AGA-1702: the longest tier-3 driver cooldown the ladder waits out (the LLM
# spacing is 180 s, so this is spacing + margin).
TIER3_COOLDOWN_WAIT_MAX = 185
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


class TrialRequest(BaseModel):
    """AGA-1461 driver bake-off request. Free-form text fields are sanitized exactly
    like /download's; driver/model/effort/budget come from the allowlists below."""
    doi: str
    title: str
    driver: str
    model: str
    effort: str | None = None
    max_budget_usd: float | None = None
    # AGA-1702: "tcd" runs the same trial through the TCD browser (bake-off before
    # production's tier3_browser is switched).
    browser: str = "local"


TRIAL_MODELS: dict[str, frozenset[str]] = {
    "codex": frozenset({"gpt-5.6-sol", "gpt-6.1-sol"}),
    "claude": frozenset({"claude-sonnet-5-5", "claude-haiku-5-5", "claude-opus-5-5"}),
}
TRIAL_EFFORTS: dict[str, frozenset[str]] = {
    "codex": frozenset({"medium", "high"}),
    # claude 2.1.312 `--effort <low|medium|high|xhigh|max>`; the bake-off uses the first three.
    "claude": frozenset({"low", "medium", "high"}),
}
TRIAL_DEFAULT_BUDGET_USD = 1.50
TRIAL_MAX_BUDGET_USD = 3.00
TRIAL_NOTES_MAX = 500
TRIAL_LEDGER_RAW_INPUT = "paperika:trial_ledger"
# What the post-trial scan looks for in the run dir's scripts/logs (policy: no
# logins, no CAPTCHA solving). Matches are REPORTED, never blocking.
_POLICY_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = tuple(
    (name, re.compile(rx, re.IGNORECASE)) for name, rx in (
        ("captcha", r"captcha"),
        ("solver", r"solver"),
        ("2captcha", r"2captcha"),
        ("anticaptcha", r"anti-?captcha"),
        ("capsolver", r"capsolver"),
        ("password", r"password"),
        ("document.cookie", r"document\.cookie"),
        ("Network.getAllCookies", r"Network\.getAllCookies"),
        ("Network.getCookies", r"Network\.getCookies"),
        ("storage_state", r"storage_state"),
    )
)
# Files the bridge itself writes into a run dir (they hold the prompt/task data or
# the model's final report, not the model's scripts).
_POLICY_SCAN_SKIP = {
    "task.json", "outcome.schema.json", "argv.json", "last_message.txt",
}
_POLICY_SCAN_SUFFIXES = {".py", ".js", ".mjs", ".sh", ".log", ".txt", ".json", ".md", ".html"}
_POLICY_SCAN_MAX_BYTES = 2_000_000
_POLICY_FLAGS_MAX = 30


class BridgeState(BaseModel):
    sandbox_mode: str | None = None
    last_selftest_ok: bool | None = None
    last_selftest_at: str | None = None
    last_variant1_failure_reason: str | None = None
    last_recovery_action: str | None = None
    claude_last_selftest_ok: bool | None = None
    claude_last_selftest_at: str | None = None
    # AGA-1462: latched on a 401 from the Elsevier API, cleared on the next 200.
    elsevier_key_invalid: bool = False
    # AGA-1702: POST /selftest/tcd results PER DRIVER: a driver runs tier 3 in TCD
    # mode only while its own result is a fresh pass.
    tcd_codex_last_selftest_ok: bool | None = None
    tcd_codex_last_selftest_at: str | None = None
    tcd_claude_last_selftest_ok: bool | None = None
    tcd_claude_last_selftest_at: str | None = None
    # AGA-1702: when /download last ran the codex selftest itself to re-arm a
    # latched-off or stale codex (at most once per CODEX_REARM_INTERVAL).
    codex_rearm_attempted_at: str | None = None


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
    "human_check_pending": ("human_check_pending", "bot_wall", 502, "human_check_pending"),
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
    # A chrome recovery skipped at the request deadline (AGA-1460): the next
    # request runs it under the lock before any browser work, even if CDP probes
    # healthy (an orphan page from the overrun run may still be alive).
    recovery_pending: bool = False

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

    def _locate_in_window(
        self, window_start: datetime, window_end: datetime, *, download_dir: Path | None = None
    ) -> Path | None:
        downloads = download_dir or self.config.download_dir
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
        self, path: Path, window_start: datetime, window_end: datetime,
        *, download_dir: Path | None = None,
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
            downloads = (download_dir or self.config.download_dir).resolve()
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

    # --- AGA-1461 trial ledger ---

    def trial_ledger_request_id(self) -> int:
        """paper_attempts.request_id is a NOT NULL FK, so trial attempt rows hang off
        ONE reserved ledger request (created lazily, status 'completed' so no retry
        or startup pass ever touches it). It is the only paper_requests row trials
        ever cause; no papers/paper_links rows are written."""
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT id FROM paper_requests WHERE raw_input = ? ORDER BY id LIMIT 1",
                (TRIAL_LEDGER_RAW_INPUT,),
            ).fetchone()
        if row:
            return int(row["id"])
        # ONE statement, already in its final state: a crash can never leave a
        # production-retryable 'queued' request behind.
        now = limits.iso(_utc_now())
        with self.db.transaction() as conn:
            cursor = conn.execute(
                "INSERT INTO paper_requests (raw_input, status, created_at, updated_at) "
                "VALUES (?, 'completed', ?, ?)",
                (TRIAL_LEDGER_RAW_INPUT, now, now),
            )
            return int(cursor.lastrowid)

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
        if (row["strategy"] or "").startswith("trial_"):
            # AGA-1461: a trial interrupted by a restart. It has no real request to
            # fail/notify and must never be linked — just close the ledger row.
            bridge.resolve_attempt(
                row["id"], outcome="interrupted", message="bridge restarted mid-trial"
            )
            continue
        # Recover doi/title from the request row for the salvage verify.
        req = bridge.db.get_request(request_id)
        doi = (req["inferred_doi"] if req else None) or ""
        title = (req["inferred_title"] if req else None) or ""
        window_start = _parse_iso(started) or _utc_now()
        window_end = window_start + timedelta(seconds=SWEEP_WINDOW_SECONDS)
        if window_end > _utc_now():
            window_end = _utc_now()

        salvaged = False
        # AGA-1702: a codex_selftest row (the latch re-arm) never downloads, so it
        # is never a salvage window; it is closed 'interrupted' like any other.
        if (doi or title) and row["strategy"] != limits.CODEX_SELFTEST_STRATEGY:
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


_STRATEGIES = (
    "direct_fetch", "scripted_browser", "codex_bridge", "claude_bridge", "elsevier_api",
    *limits.TRIAL_STRATEGIES,
)

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


# --- AGA-1702: the TCD browser as tier 3's browser ------------------------

TIER3_BROWSERS = ("local", "tcd")
TCD_DRIVERS = ("codex", "claude")


def _tcd_driver_reason(state: BridgeState, driver: str) -> str | None:
    """None when ``driver``'s OWN TCD selftest passed within the window codex's
    freshness uses, else why not. A driver runs tier 3 in TCD mode only on its own
    pass: a claude pass never puts codex in TCD mode, nor the reverse."""
    ok = getattr(state, f"tcd_{driver}_last_selftest_ok")
    if ok is None:
        return "no TCD selftest has run (POST /selftest/tcd)"
    if ok is False:
        return "the last TCD selftest failed"
    last_at = _parse_iso(getattr(state, f"tcd_{driver}_last_selftest_at"))
    if last_at is None or not (
        timedelta(0) <= _utc_now() - last_at <= timedelta(days=SELFTEST_MAX_AGE_DAYS)
    ):
        return f"the TCD selftest is older than {SELFTEST_MAX_AGE_DAYS} days"
    return None


async def _tcd_browser_reason() -> str | None:
    """None when the CLI exists and the cached status probe reports the gateway ok
    and the shared browser up, else why not."""
    if not tcd_browser.cli_present():
        return "tcd-browser CLI not found"
    status = await tcd_browser.probe_status()
    if status.error:
        return f"status probe: {status.error}"
    if not status.gateway_ok:
        return "gateway not ok"
    if not status.shared_up:
        return "shared browser not up"
    return None


async def _tcd_not_ready_reason(state: BridgeState, driver: str) -> str | None:
    """Readiness of ONE driver in TCD mode (trials): CLI, its own fresh selftest, then
    the cached status probe."""
    if not tcd_browser.cli_present():
        return "tcd-browser CLI not found"
    reason = _tcd_driver_reason(state, driver)
    if reason is not None:
        return f"{driver}: {reason}"
    return await _tcd_browser_reason()


def _configured_tier3_browser(config: PaperikaConfig) -> str:
    return (config.tier3_browser or "").strip().lower()


async def _tier3_could_run(
    bridge: Bridge, state: BridgeState, *, chrome_ok: bool, claude_ready: bool,
    remaining: Callable[[], float],
) -> dict[str, bool]:
    """Per rate key: could this driver run tier 3 once its rate rule allows it? It
    must be armed (codex) / keyed (claude), and have a browser: the local Chrome, or
    (tier3_browser = tcd) its own TCD readiness. Read-only: no socket dir, request row
    or lease (``_tcd_not_ready_reason`` is the same rule ``_select_tier3_browser``
    applies, over the cached status probe)."""
    tcd_mode = _configured_tier3_browser(bridge.config) == "tcd"
    out = {"codex_bridge": bool(state.sandbox_mode), "claude_bridge": claude_ready}
    for strat, driver in (("codex_bridge", "codex"), ("claude_bridge", "claude")):
        if out[strat] and not chrome_ok:
            out[strat] = tcd_mode and await _tcd_not_ready_reason(state, driver) is None
    if not out["codex_bridge"] and _codex_rearm_possible(bridge, state, chrome_ok, remaining):
        out["codex_bridge"] = True  # unarmed now, but the re-arm after the wait would arm it
    return out


def _codex_rearm_possible(
    bridge: Bridge, state: BridgeState, chrome_ok: bool, remaining: Callable[[], float],
) -> bool:
    """The gates ``_maybe_rearm_codex`` applies, except the rate decision (the caller
    has it): a due re-arm, the local Chrome it drives, room for the selftest plus the
    paper attempt, and a useful selftest wall in ``remaining``."""
    if not (chrome_ok and _codex_rearm_due(state)):
        return False
    with bridge.db.transaction() as conn:
        room = limits.pool_room_today(conn, "codex_bridge")
    return room >= 2 and _rearm_wall_seconds(remaining()) >= CODEX_REARM_MIN_WALL_SECONDS


async def _select_tier3_browser(
    bridge: Bridge, state: BridgeState, *, codex_can: bool, claude_can: bool,
    ensure_request: Callable[[], tuple[int, Path]],
) -> tuple[bool, bool, bool, Path | None]:
    """THIS request's tier-3 browser, decided once at the tier-3 gate. Returns
    ``(tcd, codex_tcd, claude_tcd, socket_dir)``: whether tier 3 runs in TCD mode
    (then with the run's agent-browser socket dir, created HERE: if that fails for any
    reason the request runs local like any other not-ready reason), and which
    drivers are TCD-ready (own fresh TCD selftest AND the browser answering), reported
    even when tier 3 falls back to local so the caller does not blame a local Chrome
    a TCD-ready driver would not need. ``codex_can`` / ``claude_can``: the driver
    could run this request at all (armed / key, rate rule).

    TCD mode is used when a TCD-ready driver can run: codex first when it is TCD-ready
    and can run, with claude as its fallback only if claude is TCD-ready too; else
    claude alone in TCD mode. A request never mixes a TCD run with a local one.
    Otherwise the local Chrome, exactly as before, with the reason logged once."""
    local: tuple[bool, bool, bool, Path | None] = (False, False, False, None)
    mode = _configured_tier3_browser(bridge.config)
    if mode == "local":
        return local
    if mode != "tcd":
        logger.warning("tier 3 browser: unknown tier3_browser %r; using the local Chrome", mode)
        return local

    def _fallback(reason: str) -> tuple[bool, bool, bool, Path | None]:
        logger.warning(
            "tier 3 browser: tcd configured but not ready (%s); using the local Chrome", reason
        )
        return local

    if not tcd_browser.cli_present():
        return _fallback("tcd-browser CLI not found")
    reasons = {d: _tcd_driver_reason(state, d) for d in TCD_DRIVERS}
    if all(reasons.values()):
        why = "; ".join(f"{d}: {reasons[d]}" for d in TCD_DRIVERS)
        return _fallback(f"no driver is TCD-ready ({why})")
    browser = await _tcd_browser_reason()
    if browser is not None:
        return _fallback(browser)
    codex_tcd, claude_tcd = reasons["codex"] is None, reasons["claude"] is None
    if not ((codex_tcd and codex_can) or (claude_tcd and claude_can)):
        why = "; ".join(
            f"{d}: {reasons[d] or 'TCD-ready but cannot run this request'}" for d in TCD_DRIVERS
        )
        _fallback(f"no TCD-ready driver can run ({why})")
        return False, codex_tcd, claude_tcd, None
    # The run's agent-browser socket dir, created once, here, on the real run dir (the
    # request is materialized for it; tier 3 needs it anyway). Any failure: local.
    _request_id, run_dir = ensure_request()
    socket_dir = tcd_browser.make_socket_dir(run_dir)  # never raises
    if socket_dir is None:
        return _fallback(
            "no agent-browser socket dir could be created within the unix socket path limit"
        )
    if codex_tcd and codex_can and claude_can and not claude_tcd:
        logger.warning(
            "tier 3 browser: tcd for codex; claude is not TCD-ready (%s), so no claude "
            "fallback in this request", reasons["claude"],
        )
    return True, codex_tcd, claude_tcd, socket_dir


# --- AGA-1702: the codex readiness latch re-arms itself ---------------------

# A runtime auth error (or a failed/stale selftest) leaves codex off until someone
# re-runs POST /selftest/codex; it once stayed off nine days after the login was
# fixed. /download now runs that selftest itself, at most once per this interval.
CODEX_REARM_INTERVAL = timedelta(hours=24)
# Wall of the one sandboxed selftest the re-arm runs. A passing selftest measures
# ~70 s at high effort.
CODEX_REARM_WALL_SECONDS = 180
CODEX_REARM_MIN_WALL_SECONDS = 60


def _codex_rearm_due(state: BridgeState) -> bool:
    """Codex is off or stale, a selftest has run before (the FIRST arm stays an
    operator step: POST /selftest/codex), the persisted mode is not ``bypass`` (the
    re-arm only ever arms the sandboxed mode, and leaves a bypass state alone), and
    no selftest, manual or automatic, was attempted within CODEX_REARM_INTERVAL."""
    if _parse_iso(state.last_selftest_at) is None or _codex_usable(state):
        return False
    if state.sandbox_mode == "bypass":
        return False
    now = _utc_now()
    for stamp in (state.last_selftest_at, state.codex_rearm_attempted_at):
        at = _parse_iso(stamp)
        if at is not None and timedelta(0) <= now - at < CODEX_REARM_INTERVAL:
            return False
    return True


def _rearm_wall_seconds(remaining: float) -> float:
    """The wall the re-arm selftest would get with ``remaining`` seconds of budget left."""
    return min(
        float(CODEX_REARM_WALL_SECONDS),
        remaining - CLEANUP_RESERVE_SECONDS - FALLBACK_MIN_SECONDS - executor.KILL_GRACE_SECONDS,
    )


async def _maybe_rearm_codex(
    bridge: Bridge, state: BridgeState, *, deadline: float, chrome_ok: bool, codex_rate_ok: bool,
    ensure_request: Callable[[], tuple[int, Path]],
) -> BridgeState:
    """At the tier-3 gate of a /download (under the lock), run the codex selftest's
    ``workspace-write`` variant ONCE when ``_codex_rearm_due`` and carry on with its
    result. It never runs the bypass variant and never persists ``bypass``.

    The launch is a ledger row (``limits.CODEX_SELFTEST_STRATEGY`` on the current
    request, pooled with codex_bridge), so it spends codex's daily cap; it runs only
    when the pool has room for it AND the paper attempt after it. Skipped too when
    codex could not run anyway (rate-blocked), when the selftest could not pass (it
    drives the LOCAL Chrome, so it needs chrome_ok), and when the request budget
    cannot fit it and still leave tier 3 a useful wall. Never runs from /healthz."""
    if not (codex_rate_ok and chrome_ok and _codex_rearm_due(state)):
        return state
    with bridge.db.transaction() as conn:
        room = limits.pool_room_today(conn, "codex_bridge")
    if room < 2:
        logger.warning(
            "codex latch re-arm skipped: the codex pool has room for %d launch(es) today, "
            "not the selftest plus the paper attempt", max(room, 0),
        )
        return state
    remaining = deadline - MONOTONIC()
    wall = _rearm_wall_seconds(remaining)
    if wall < CODEX_REARM_MIN_WALL_SECONDS:
        logger.info("codex latch re-arm skipped: request budget too short (%.0fs left)", remaining)
        return state
    request_id, _run_dir = ensure_request()
    selftest_dir = bridge.config.db_path.parent / "codex_runs" / "selftest"
    # Recorded BEFORE the run (state stamp + ledger row), so a crash or cancel
    # mid-selftest cannot retry it within the interval, and the launch is counted.
    bridge.state_store.update(codex_rearm_attempted_at=_utc_now().isoformat())
    attempt_id = bridge.write_ahead(
        request_id=request_id, paper_id=None, run_dir=str(selftest_dir),
        strategy=limits.CODEX_SELFTEST_STRATEGY,
    )
    logger.warning("codex readiness latch is off or stale; running the sandboxed codex selftest once")
    try:
        result = await executor.run_selftest_sandboxed(selftest_dir, wall_seconds=wall)
    except Exception as exc:
        logger.exception("codex latch re-arm: the selftest could not run")
        bridge.resolve_attempt(attempt_id, outcome="selftest_failed",
                               message=f"selftest could not run: {type(exc).__name__}")
        return bridge.state_store.load()
    except BaseException:
        bridge.resolve_attempt(attempt_id, outcome="interrupted", message="selftest interrupted")
        raise
    now_iso = _utc_now().isoformat()
    if result.ok:
        # What POST /selftest/codex persists on a pass (the mode is always workspace-write).
        bridge.state_store.update(
            sandbox_mode=result.sandbox_mode, last_selftest_ok=True, last_selftest_at=now_iso,
            last_variant1_failure_reason=None,
        )
        bridge.resolve_attempt(attempt_id, outcome="selftest_ok", message="codex latch re-armed")
        logger.warning("codex latch re-armed (sandbox_mode=%s)", result.sandbox_mode)
    else:
        # As POST /selftest/codex on a failure: sandbox_mode is left as it was, except
        # that a dead login clears it, as a runtime auth_error does.
        fields: dict[str, Any] = {"last_selftest_ok": False, "last_selftest_at": now_iso}
        if result.error_code == "codex_auth":
            fields["sandbox_mode"] = None
        bridge.state_store.update(**fields)
        bridge.resolve_attempt(attempt_id, outcome="selftest_failed",
                               message=f"{result.error_code}: {result.detail}"[:300])
        logger.warning("codex latch re-arm failed: %s %s", result.error_code, result.detail)
    return bridge.state_store.load()


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
        expiry = asyncio.create_task(_human_check_expiry_loop(bridge))
        try:
            yield
        finally:
            expiry.cancel()
            try:
                await expiry
            except asyncio.CancelledError:
                pass

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
        trial_cost = None
        attempts_by_strategy: dict[str, int | None] = {
            strat: None for strat in _STRATEGIES
        }
        tcd_attempts: dict[str, int | None] = {s: None for s in limits.TCD_STRATEGIES}
        tcd_claude_cost: float | None = None
        tcd_trial_cost: float | None = None
        if db_ok:
            with bridge.db.transaction() as conn:
                downloads_today = limits.attempts_today(conn)
                attempts_by_strategy = {
                    strat: limits.attempts_today(conn, strat)
                    for strat in _STRATEGIES
                }
                claude_cost = limits.month_cost_usd(conn, "claude_bridge")
                trial_cost = limits.month_cost_usd(conn, "trial_claude")
                tcd_attempts = {s: limits.attempts_today(conn, s) for s in limits.TCD_STRATEGIES}
                tcd_claude_cost = limits.month_cost_usd(conn, "tcd_claude")
                tcd_trial_cost = limits.month_cost_usd(conn, "trial_tcd_claude")
        # AGA-1702: the CLI's status probe runs only in this authenticated tier, only
        # when tier3_browser is tcd and the CLI exists (cached
        # tcd_browser.STATUS_CACHE_TTL_SECONDS). In local mode gateway and tcd_usable
        # are null: nothing was probed.
        tcd_cli = tcd_browser.cli_present()
        tcd_mode = _configured_tier3_browser(bridge.config) == "tcd"
        tcd_status = await tcd_browser.probe_status() if (tcd_cli and tcd_mode) else None
        tcd_block = {
            "configured_mode": bridge.config.tier3_browser,
            "cli_present": tcd_cli,
            "gateway": None if tcd_status is None else {
                "ok": tcd_status.gateway_ok,
                "shared_up": tcd_status.shared_up,
                "error": tcd_status.error,
                "probe_age_seconds": int(tcd_status.age_seconds() or 0),
            },
            # True when the browser answers and at least one driver is TCD-ready.
            "tcd_usable": (
                None if not tcd_mode
                else bool(tcd_status and tcd_status.ok
                          and any(_tcd_driver_reason(state, d) is None for d in TCD_DRIVERS))
            ),
            "drivers": {
                d: {
                    "last_selftest_ok": getattr(state, f"tcd_{d}_last_selftest_ok"),
                    "last_selftest_at": getattr(state, f"tcd_{d}_last_selftest_at"),
                    "selftest_fresh": _tcd_driver_reason(state, d) is None,
                }
                for d in TCD_DRIVERS
            },
            "attempts_today": tcd_attempts,
            "claude_cost_usd": tcd_claude_cost,
            "trial_claude_cost_usd": tcd_trial_cost,
        }
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
                "model": executor.CODEX_MODEL_ID,
                "reasoning_effort": executor.CODEX_REASONING_EFFORT,
                "last_selftest_ok": state.last_selftest_ok,
                "last_selftest_at": state.last_selftest_at,
                "sandbox_mode": state.sandbox_mode,
            },
            "claude": {
                "key_present": bool(executor.anthropic_api_key()),
                "model": executor.CLAUDE_MODEL,
                "claude_usable": _claude_usable(state),
                "last_selftest_ok": state.claude_last_selftest_ok,
                "last_selftest_at": state.claude_last_selftest_at,
                "claude_cost_usd": claude_cost,
                "trial_cost_usd": trial_cost,
            },
            "elsevier": {
                "key_present": bool(executor.elsevier_api_key()),
                "key_invalid": bool(state.elsevier_key_invalid),
                "attempts_today": attempts_by_strategy.get("elsevier_api"),
            },
            "tcd": tcd_block,
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

    # --- /selftest/tcd (AGA-1702) ---

    @app.post("/selftest/tcd")
    async def selftest_tcd(request: Request, driver: str | None = None) -> Response:
        """One executor in TCD mode opens https://example.org through the CLI and
        reports its title and URL; passes only on the exact expected pair. Driver:
        ``?driver=codex|claude``, else codex when its latch is armed, else claude."""
        denied = require_token(request)
        if denied is not None:
            return denied
        if driver is not None and driver not in TRIAL_MODELS:
            return _json_error(422, "invalid_driver", "driver must be codex or claude")
        if not tcd_browser.cli_present():
            return _json_error(503, "tcd_unavailable", "tcd-browser CLI not found", ok=False)
        if bridge.lock.locked():
            return _json_error(429, "busy", "a download or selftest is in progress")
        async with bridge.lock:
            state = bridge.state_store.load()
            api_key = executor.anthropic_api_key()
            claude_ok = _claude_ready(state) and bool(api_key)
            if driver is None:
                driver = "codex" if state.sandbox_mode else ("claude" if claude_ok else None)
            if driver is None or (driver == "codex" and not state.sandbox_mode):
                return _json_error(503, "selftest_required", "run POST /selftest/codex first",
                                   ok=False)
            if driver == "claude" and not claude_ok:
                return _json_error(503, "claude_auth", "claude driver is not available", ok=False)
            return await _tcd_selftest_locked(bridge, driver=driver, state=state, api_key=api_key)

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

    # --- /admin/trial (AGA-1461) ---

    @app.post("/admin/trial")
    async def admin_trial(payload: TrialRequest, request: Request) -> Response:
        denied = require_token(request)
        if denied is not None:
            return denied
        doi = bridge.normalize_doi(payload.doi)
        if doi is None:
            return _json_error(422, "invalid_doi", "doi must match ^10\\.\\d{4,9}/\\S+$")
        title = bridge.sanitize_title(payload.title)
        driver = payload.driver
        if driver not in TRIAL_MODELS:
            return _json_error(422, "invalid_driver", "driver must be codex or claude")
        if payload.model not in TRIAL_MODELS[driver]:
            return _json_error(
                422, "invalid_model",
                f"model for {driver} must be one of {sorted(TRIAL_MODELS[driver])}",
            )
        if payload.effort is not None and payload.effort not in TRIAL_EFFORTS[driver]:
            return _json_error(
                422, "invalid_effort",
                f"effort for {driver} must be null or one of {sorted(TRIAL_EFFORTS[driver])}",
            )
        budget: float | None = None
        if driver == "claude":
            budget = (
                TRIAL_DEFAULT_BUDGET_USD if payload.max_budget_usd is None
                else payload.max_budget_usd
            )
            if not (0 < budget <= TRIAL_MAX_BUDGET_USD):
                return _json_error(
                    422, "invalid_budget",
                    f"max_budget_usd must be in (0, {TRIAL_MAX_BUDGET_USD:.2f}]",
                )
        elif payload.max_budget_usd is not None:
            return _json_error(422, "invalid_budget", "max_budget_usd applies to claude only")
        if payload.browser not in TIER3_BROWSERS:
            return _json_error(422, "invalid_browser", "browser must be local or tcd")

        if bridge.lock.locked():
            return _json_error(429, "busy", "a download or trial is in progress")
        async with bridge.lock:
            try:
                return await _trial_locked(
                    bridge, doi=doi, title=title, driver=driver, model=payload.model,
                    effort=payload.effort, max_budget_usd=budget, browser=payload.browser,
                )
            except Exception as exc:  # CancelledError is a BaseException: not caught
                logger.exception("unhandled exception in /admin/trial (doi=%s)", doi)
                return _json_error(
                    _TAXONOMY["bridge_error"][2], _TAXONOMY["bridge_error"][3],
                    _redact_all(f"{type(exc).__name__}: {exc}")[:300],
                )

    return app


async def _tcd_selftest_locked(
    bridge: Bridge, *, driver: str, state: BridgeState, api_key: str | None,
) -> Response:
    """POST /selftest/tcd's run, under the lock (AGA-1702). A result the executor
    returned is persisted for the driver that ran even when the request is cancelled
    during the close; that cancellation is re-raised only afterwards."""
    status = await tcd_browser.probe_status(fresh=True)
    if not status.ok:
        why = status.error or ("gateway not ok" if not status.gateway_ok
                               else "shared browser not up")
        return _json_error(503, "tcd_unavailable", f"tcd-browser status: {why}", ok=False)
    run_dir = bridge.config.db_path.parent / "codex_runs" / "selftest_tcd"
    session = tcd_browser.session_name(f"selftest{int(time.time())}", driver)
    socket_dir = tcd_browser.make_socket_dir(run_dir)
    if socket_dir is None:
        return _json_error(503, "tcd_unavailable",
                           "no agent-browser socket dir fits the unix socket path limit", ok=False)
    try:
        # Pass evidence beyond the model's own report: the screenshot its one chained
        # command wrote (checked in run_tcd_selftest) and, below, the RELEASED close.
        result = await executor.run_tcd_selftest(
            run_dir, driver=driver, env=tcd_browser.session_env(session, socket_dir),
            sandbox_mode=state.sandbox_mode, api_key=api_key,
        )
    except BaseException:
        await tcd_browser.close_session(session, socket_dir)
        raise
    closed = await tcd_browser.close_session(session, socket_dir, defer_cancel=True)
    late_cancel = closed.cancelled
    if result.ok and closed.outcome != tcd_browser.RELEASED:
        # A pass needs its own lease confirmed released ("left to expire" or an
        # unclassified close is not that).
        result = executor.SelftestResult(
            False, error_code="executor_failed", wall_seconds=result.wall_seconds,
            detail=f"the selftest's lease could not be confirmed released ({closed.outcome})",
        )
    # Only the driver that ran: its pass never makes the other TCD-ready.
    bridge.state_store.update(**{
        f"tcd_{driver}_last_selftest_ok": result.ok,
        f"tcd_{driver}_last_selftest_at": _utc_now().isoformat(),
    })
    if late_cancel is not None:
        raise late_cancel  # only now: the result above is already persisted
    if result.ok:
        return JSONResponse(
            status_code=200,
            content={"ok": True, "driver": driver, "wall_seconds": round(result.wall_seconds, 2)},
        )
    detail = _redact_all(result.detail)
    if result.error_code in {"codex_auth", "claude_auth"}:
        return _json_error(503, result.error_code, detail, ok=False, driver=driver)
    return _json_error(502, "executor_failed", detail, ok=False, driver=driver)


async def _chrome_preflight(bridge: Bridge) -> bool:
    """Chrome health gate shared by /download and /admin/trial (under the lock):
    run a deferred recovery first, recycle a stale browser, probe, and recover once
    if sick. Returns whether browser work may proceed."""
    if bridge.recovery_pending:
        rec = await chrome.recover()
        bridge.state_store.update(last_recovery_action=rec.action)
        # Cleared only by a successful recovery; until then the browser tiers are
        # gated off as if Chrome were down (an orphan page may still be alive).
        bridge.recovery_pending = not rec.recovered
    await chrome.idle_recycle_if_stale()
    probe = await chrome.probe(fresh=True)
    chrome_ok = probe.ok and not bridge.recovery_pending
    if not chrome_ok:
        rec = await chrome.recover()
        if rec.recovered:
            bridge.recovery_pending = False
        probe = await chrome.probe(fresh=True)
        chrome_ok = probe.ok and not bridge.recovery_pending
    return chrome_ok


_HUMAN_CHECK_PENDING_REASON = "waiting for Agastya to pass a human check in the shared browser"
_HUMAN_CHECK_NEXT_ACTION = (
    "A request is waiting in the tcd-browser extension: open it and pass the check. "
    "The download is tried again automatically; nothing needs to be saved by hand."
)


def _human_check_reason(title: str) -> str:
    return f"Paper download: {title[:80]}. Pass the human check; the download finishes by itself."


def _human_check_task(title: str) -> str:
    """The name his inbox shows for the request: the paper."""
    return title.strip()[:60] or "Paper download"


# How often unanswered requests are checked for their maximum age.
HUMAN_CHECK_SWEEP_SECONDS = 600


async def _expire_human_checks(bridge: Bridge) -> int:
    """Withdraw every request to him that has waited past the maximum age, so an
    unanswered one never stays on his screen after the caller has given up. The
    attempt is closed as a wall; a later request for the paper starts afresh.

    Runs under the download lock, and only when it is free: no /download can then
    file a new ask for one of these URLs between the selection and the withdrawal
    (a late withdrawal would take the new ask off his screen and end that download
    as declined). A pass that finds the lock held leaves the work to the next one.
    """
    if bridge.config.human_check_wait_seconds <= 0 or _configured_tier3_browser(bridge.config) != "tcd":
        return 0
    if bridge.lock.locked():
        return 0
    max_age = timedelta(hours=bridge.config.human_check_max_age_hours)
    expired = 0
    async with bridge.lock:
        now = _utc_now()
        with bridge.db.transaction() as conn:
            rows = conn.execute(
                "SELECT a.id, a.finished_at, a.current_url, r.manual_current_url, r.inferred_url "
                "FROM paper_requests r JOIN paper_attempts a ON a.id = "
                "(SELECT MAX(id) FROM paper_attempts WHERE request_id = r.id) "
                "WHERE r.status = 'failed' AND a.outcome = 'human_check_pending' "
                "AND a.strategy IN ('tcd_codex', 'tcd_claude')",
            ).fetchall()
        for row in rows:
            pending_at = _parse_iso(row["finished_at"])
            if pending_at is None or now - pending_at <= max_age:
                continue
            url = row["manual_current_url"] or row["current_url"] or row["inferred_url"]
            withdrawn = await tcd_browser.ask_human_check(url, withdraw=True) == "done"
            if not withdrawn and now - pending_at <= 2 * max_age:
                continue  # the ask may still be on his screen: the next pass tries again
            bridge.resolve_attempt(
                row["id"], outcome="bot_wall",
                message="human check request expired" if withdrawn
                else "human check request expired; its withdrawal failed")
            expired += 1
    return expired


async def _human_check_expiry_loop(bridge: Bridge) -> None:
    while True:
        # asyncio.sleep, not SLEEP: the loop must not spin under a stubbed clock.
        await asyncio.sleep(HUMAN_CHECK_SWEEP_SECONDS)
        try:
            await _expire_human_checks(bridge)
        except Exception:  # noqa: BLE001 - the next pass tries again
            logger.warning("human check expiry pass failed", exc_info=True)


async def _check_pending_human_check(bridge: Bridge, *, doi: str, title: str) -> tuple[Response | None, bool]:
    """Poll only the latest request for this DOI, before any tier or Chrome work.
    Repeated pending answers do not rewrite its timestamp or extend its lifetime."""
    if bridge.config.human_check_wait_seconds <= 0 or _configured_tier3_browser(bridge.config) != "tcd":
        return None, False
    with bridge.db.transaction() as conn:
        row = conn.execute(
            "SELECT r.id AS request_id, r.status AS request_status, r.inferred_url, "
            "r.manual_current_url, r.manual_screenshot_path, a.* "
            "FROM paper_requests r LEFT JOIN paper_attempts a ON a.id = "
            "(SELECT MAX(id) FROM paper_attempts WHERE request_id = r.id) "
            "WHERE r.inferred_doi = ? ORDER BY r.id DESC LIMIT 1", (doi,),
        ).fetchone()
    if (row is None or row["request_status"] != "failed" or row["outcome"] != "human_check_pending"
            or row["strategy"] not in {"tcd_codex", "tcd_claude"}):
        return None, False
    url = row["manual_current_url"] or row["current_url"] or row["inferred_url"]
    pending_at = _parse_iso(row["finished_at"])
    if pending_at is not None and _utc_now() - pending_at > timedelta(hours=bridge.config.human_check_max_age_hours):
        await tcd_browser.ask_human_check(url, withdraw=True)
        bridge.resolve_attempt(row["id"], outcome="bot_wall", message="human check request expired")
        return None, False
    result = await tcd_browser.ask_human_check(
        url, reason=_human_check_reason(title), task=_human_check_task(title), wait_seconds=0)
    if result in {"pending", "unavailable"}:
        # A poll that could not reach the gateway says nothing about his answer:
        # the request stays open on his screen, so the paper keeps waiting.
        return _json_error(
            502, "human_check_pending", _HUMAN_CHECK_PENDING_REASON,
            request_id=str(row["request_id"]), screenshot=row["manual_screenshot_path"],
            retry_after_seconds=bridge.config.human_check_retry_seconds,
        ), False
    if result == "done":
        bridge.resolve_attempt(row["id"], outcome="bot_wall", message="human check passed; resuming download")
        return None, True  # normal ladder; a returning wall must not ask again
    return _finish_failure(
        bridge, kind="bot_wall", request_id=row["request_id"], attempt_id=row["id"],
        screenshot=row["manual_screenshot_path"], final_url=url, message="bot_wall",
        wall_reason="he declined the human check" if result == "declined" else None,
    ), False


async def _retry_after_human_check(
    bridge: Bridge, *, request_id: int, run_dir: Path, doi: str, title: str,
    start_url: str, deadline: float,
) -> Response | None:
    """ONE extra TCD attempt, under the usual readiness, pooled caps, spacing and
    deadline. No driver available (including after a short cooldown): stay pending.
    There is no second fallback chain and no second human ask."""
    state = bridge.state_store.load()
    ready = []
    for driver, armed in (("codex", bool(state.sandbox_mode)), ("claude", _claude_ready(state))):
        if armed and await _tcd_not_ready_reason(state, driver) is None:
            ready.append(driver)

    def decisions() -> dict[str, limits.RateDecision]:
        with bridge.db.transaction() as conn:
            return {driver: limits.check_rate(conn, f"tcd_{driver}") for driver in ready
                    if not limits.pool_at_daily_cap(conn, f"tcd_{driver}")}

    rate = decisions()
    if not any(d.allowed for d in rate.values()):
        waits = [d.retry_after_seconds for d in rate.values() if d.error_code == "cooldown"
                 and d.retry_after_seconds is not None and d.retry_after_seconds <= TIER3_COOLDOWN_WAIT_MAX]
        if waits and deadline - MONOTONIC() - min(waits) >= FALLBACK_MIN_SECONDS + CLEANUP_RESERVE_SECONDS:
            await SLEEP(min(waits))
            rate = decisions()
    driver = next((d for d in ready if d in rate and rate[d].allowed), None)
    wall = min(float(executor.EXEC_WALL_SECONDS), deadline - MONOTONIC() - CLEANUP_RESERVE_SECONDS)
    if driver is None or wall < FALLBACK_MIN_SECONDS:
        return None
    socket_dir = tcd_browser.make_socket_dir(run_dir)
    if socket_dir is None:
        return None
    return await _run_executor_attempt(
        bridge, driver=driver, request_id=request_id, run_dir=run_dir, doi=doi,
        title=title, start_url=start_url, sandbox_mode=state.sandbox_mode if driver == "codex" else None,
        defer_failure=False, deadline=deadline, api_key=executor.anthropic_api_key() if driver == "claude" else None,
        codex_wall=wall, claude_wall=wall, tcd=True, socket_dir=socket_dir, human_check_passed=True,
    )


async def _download_locked(
    bridge: Bridge, *, doi: str, title: str, start_url: str, requested_by: str
) -> Response:
    """The tiered download ladder (AGA-339). Deterministic tier 1 (direct HTTP
    fetch) and tier 2 (scripted Playwright) run BEFORE the tier-3 codex executor,
    each rate-disciplined on its OWN strategy. Tiers 1/2 only ever succeed or
    escalate — codex stays the sole terminal-classification authority, except a
    both-tiers bot wall with codex unavailable, mapped to a retryable 502
    bot_wall_unclassified (AGA-596 P1)."""
    pending_response, human_check_passed = await _check_pending_human_check(bridge, doi=doi, title=title)
    if pending_response is not None:
        return pending_response
    state = bridge.state_store.load()
    deadline = MONOTONIC() + REQUEST_BUDGET_SECONDS

    # 4. rate pre-flight across all three strategies. A tier whose strategy is
    # rate-blocked is silently skipped (no attempt row). Only when EVERY tier is
    # blocked do we 429 — with the smallest retry_after among them.
    claude_ready = _claude_ready(state)
    rate_strats = ("direct_fetch", "scripted_browser", "codex_bridge") + (
        ("claude_bridge",) if claude_ready else ()
    )
    # AGA-1462: the Elsevier API step counts as an available tier when it applies
    # (10.1016/ DOI, valid key, its own rate rule allows it).
    els_key = executor.elsevier_api_key() if elsevier_api.applies_to(doi) else None
    with bridge.db.transaction() as conn:
        rate = {strat: limits.check_rate(conn, strat) for strat in rate_strats}
        els_allowed = bool(els_key) and limits.check_rate(conn, "elsevier_api").allowed
    regular_blocked = all(not d.allowed for d in rate.values())
    if regular_blocked and not els_allowed:
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
    chrome_ok = await _chrome_preflight(bridge)

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

    # --- AGA-1462: Elsevier Article Retrieval API, before tier 1, 10.1016/ DOIs only.
    # Deterministic; success returns, every miss falls through (never a verdict).
    # An absent key or a rate-blocked strategy skips it silently.
    if els_allowed and els_key:
        request_id, run_dir = ensure_request()
        outcome = await _run_deterministic_tier(
            bridge, strategy="elsevier_api",
            fetch_fn=functools.partial(
                elsevier_api.attempt_elsevier_fetch, api_key=els_key,
                set_key_invalid=lambda bad: _set_elsevier_key_invalid(bridge, bad),
            ),
            budget=elsevier_api.BUDGET_SECONDS, request_id=request_id,
            run_dir=run_dir, doi=doi, title=title, start_url=start_url,
        )
        if isinstance(outcome, Response):
            return outcome
        tier_kinds["elsevier_api"] = outcome
    if all(not d.allowed for d in rate.values()):
        # Only the Elsevier step was available and it missed. Recomputed, not the
        # pre-flight flag: the tier-1 cooldown wait above may have freed tier 1.
        return finalize_if_created(
            _json_error(429, "rate_limited", "all download tiers are rate-limited")
        )

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
    # AGA-1702: tier 3 merely cooling down is not a reason to refuse. The pre-flight
    # decisions are stale by now (tiers 1+2 ran up to ~105 s), and back-to-back
    # requests landed inside both drivers' 180 s spacing: 2 of 12 real requests on
    # 2026-11-12 ended 429 cooldown / 502 bot_wall_unclassified with no tier-3 attempt.
    # Re-check fresh; if no usable driver is allowed, wait out the shortest short
    # cooldown (the lock is held either way) and re-check. A daily cap is never waited.
    with bridge.db.transaction() as conn:
        for strat in ("codex_bridge", "claude_bridge"):
            if strat in rate:
                rate[strat] = limits.check_rate(conn, strat)
    could_run = await _tier3_could_run(
        bridge, state, chrome_ok=chrome_ok, claude_ready=claude_ready,
        remaining=lambda: deadline - MONOTONIC(),
    )
    if not any(rate[s].allowed for s in could_run if could_run[s]):
        with bridge.db.transaction() as conn:
            # check_rate reports a cooldown before the daily cap: a capped driver
            # must not be the one the wait is spent on.
            capped = {s for s in could_run if limits.pool_at_daily_cap(conn, s)}
        waits = [
            rate[s].retry_after_seconds for s in could_run
            if could_run[s] and s not in capped
            and rate[s].error_code == "cooldown"
            and rate[s].retry_after_seconds is not None
            and rate[s].retry_after_seconds <= TIER3_COOLDOWN_WAIT_MAX
            # a codex that only a re-arm would arm needs the re-arm's budget AFTER the wait
            and (s != "codex_bridge" or state.sandbox_mode or _rearm_wall_seconds(
                deadline - MONOTONIC() - rate[s].retry_after_seconds) >= CODEX_REARM_MIN_WALL_SECONDS)
        ]
        if waits:
            wait = min(waits)
            if deadline - MONOTONIC() - wait >= FALLBACK_MIN_SECONDS + CLEANUP_RESERVE_SECONDS:
                await SLEEP(wait)
                with bridge.db.transaction() as conn:
                    for strat in ("codex_bridge", "claude_bridge"):
                        if strat in rate:
                            rate[strat] = limits.check_rate(conn, strat)
    # AGA-1702: re-arm a latched-off / stale codex once per 24 h, then pick the
    # browser tier 3 drives. A TCD-mode tier 3 does not need the local Chrome (tier 2
    # above keeps its own Chrome rule).
    state = await _maybe_rearm_codex(
        bridge, state, deadline=deadline, chrome_ok=chrome_ok,
        codex_rate_ok=rate["codex_bridge"].allowed, ensure_request=ensure_request,
    )
    codex_can = rate["codex_bridge"].allowed and bool(state.sandbox_mode)
    claude_can = claude_ready and rate["claude_bridge"].allowed
    tcd, codex_tcd, claude_tcd, socket_dir = await _select_tier3_browser(
        bridge, state, codex_can=codex_can, claude_can=claude_can, ensure_request=ensure_request,
    )
    browser_ok = tcd or chrome_ok
    if tcd:
        # Per-driver readiness: only a TCD-ready driver runs, and never a local one
        # beside it (no claude fallback when only codex is TCD-ready).
        codex_ready = codex_can and codex_tcd
        claude_go = claude_can and claude_tcd
    else:
        codex_ready = codex_can and chrome_ok
        claude_go = claude_can and chrome_ok
    codex_wall = float(executor.EXEC_WALL_SECONDS)
    if codex_ready or claude_go:
        # AGA-1462: the deadline caps codex's wall exactly like claude's. Too little
        # left for a useful run (< FALLBACK_MIN_SECONDS of wall): tier 3 is not run
        # and the request ends retryably, as for a rate-skipped tier.
        remaining = deadline - MONOTONIC()
        if codex_ready and claude_go:
            # AGA-1461: leave claude a guaranteed window after a full codex timeout
            # (see CLAUDE_FALLBACK_WINDOW_SECONDS). If that leaves codex too little
            # to be useful, claude runs alone.
            capped = min(
                codex_wall,
                remaining - CLEANUP_RESERVE_SECONDS
                - (CLAUDE_FALLBACK_WINDOW_SECONDS + CLEANUP_RESERVE_SECONDS),
            )
            codex_ready = capped >= FALLBACK_MIN_SECONDS
            codex_wall = capped
        elif codex_ready:
            codex_wall = min(codex_wall, remaining - CLEANUP_RESERVE_SECONDS)
        # The wall of whichever driver runs first must be a useful one.
        first_wall = codex_wall if codex_ready else remaining - CLEANUP_RESERVE_SECONDS
        if first_wall < FALLBACK_MIN_SECONDS:
            return finalize_if_created(
                _json_error(429, "cooldown", "request budget exhausted before tier 3",
                            retry_after_seconds=TIER3_SKIP_RETRY_SECONDS)
            )
        request_id, run_dir = ensure_request()
        return await _run_tier3(
            bridge, request_id=request_id, run_dir=run_dir, doi=doi, title=title,
            start_url=start_url, sandbox_mode=state.sandbox_mode if codex_ready else None,
            claude_go=claude_go, deadline=deadline, codex_wall=codex_wall, tcd=tcd,
            socket_dir=socket_dir,
            human_check_passed=human_check_passed,
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
    if not (browser_ok or codex_tcd):  # a TCD-ready codex never needed the local Chrome
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
    bridge: Bridge, file_path: str | None, window_start: datetime, window_end: datetime,
    *, download_dir: Path | None = None,
) -> Path | None:
    """Resolve a reported download to a trusted on-disk path (shared by codex and
    the deterministic tiers): accept the reported file ONLY when it passes the
    containment + mtime-window gate, else fall back to the window locator."""
    if file_path:
        p = Path(file_path)
        if bridge._within_download_window(p, window_start, window_end, download_dir=download_dir):
            return p
    return bridge._locate_in_window(window_start, window_end, download_dir=download_dir)


def _set_elsevier_key_invalid(bridge: Bridge, invalid: bool) -> None:
    """Persist the 401 latch; write only on a change (a 200 clears it)."""
    if bridge.state_store.load().elsevier_key_invalid != invalid:
        bridge.state_store.update(elsevier_key_invalid=invalid)


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
    codex_wall: float = float(executor.EXEC_WALL_SECONDS), tcd: bool = False,
    socket_dir: Path | None = None,
    human_check_passed: bool = False,
) -> Response:
    """Tier 3 driver chain (AGA-1460): codex first when usable, claude as the
    automatic fallback. ``sandbox_mode=None`` means codex is unusable (no selftest /
    rate-blocked / chrome) and claude runs directly. ``tcd`` (AGA-1702): both
    drivers use the TCD browser for this request; the first driver gets the socket
    dir the selection created, a claude fallback a fresh one (none: no fallback)."""
    pending: dict[str, Any] = {}
    if sandbox_mode is not None:
        response = await _run_executor_attempt(
            bridge, driver="codex", request_id=request_id, run_dir=run_dir, doi=doi,
            title=title, start_url=start_url, sandbox_mode=sandbox_mode,
            defer_failure=claude_go, deadline=deadline, pending=pending, codex_wall=codex_wall,
            tcd=tcd, socket_dir=socket_dir,
            human_check_passed=human_check_passed,
        )
        socket_dir = None  # consumed: codex's close released (and on success removed) it
        if response is not None:
            return response
    # Codex died / is unusable and claude is ready. Re-read the budget NOW — after
    # codex's salvage and Chrome recovery, which can eat it — immediately before
    # the claude attempt row exists. Too little left: codex's retryable failure
    # stands and no attempt row is consumed.
    remaining = deadline - MONOTONIC()
    # AGA-1461: claude must get its guaranteed window (wall >= 300 s), the same one
    # codex's capped wall reserved for it.
    if pending and remaining - CLEANUP_RESERVE_SECONDS < CLAUDE_FALLBACK_WINDOW_SECONDS:
        return _finish_failure(bridge, request_id=request_id, **pending)
    api_key = executor.anthropic_api_key()
    assert api_key  # claude_go implies a key
    if tcd and socket_dir is None:
        # The claude FALLBACK in TCD mode needs its own socket dir. If it cannot have
        # one, there is no fallback (never a local claude beside a TCD codex).
        socket_dir = tcd_browser.make_socket_dir(run_dir)  # never raises
        if socket_dir is None:
            logger.warning("tier 3 browser: no agent-browser socket dir for the claude "
                           "fallback; codex's failure stands")
            return _finish_failure(bridge, request_id=request_id, **pending)
    response = await _run_executor_attempt(
        bridge, driver="claude", request_id=request_id, run_dir=run_dir, doi=doi,
        title=title, start_url=start_url, sandbox_mode=None, defer_failure=False,
        api_key=api_key, deadline=deadline,
        claude_wall=max(1.0, min(float(executor.EXEC_WALL_SECONDS),
                                 remaining - CLEANUP_RESERVE_SECONDS)),
        tcd=tcd, socket_dir=socket_dir,
        human_check_passed=human_check_passed,
    )
    assert response is not None
    return response


async def _run_executor_attempt(
    bridge: Bridge, *, driver: str, request_id: int, run_dir: Path, doi: str, title: str,
    start_url: str, sandbox_mode: str | None, defer_failure: bool, deadline: float, api_key: str | None = None,
    claude_wall: float = float(executor.EXEC_WALL_SECONDS),
    codex_wall: float = float(executor.EXEC_WALL_SECONDS),
    pending: dict[str, Any] | None = None,
    tcd: bool = False,
    socket_dir: Path | None = None,
    human_check_passed: bool = False,
) -> Response | None:
    """One tier-3 executor attempt (``driver`` = codex | claude). Writes its own
    write-ahead row, runs the executor, resolves the terminal taxonomy (§2.7) — the
    only tier that produces terminal wrong_paper / throttled / paywalled / bot_wall /
    gave_up classifications. With ``defer_failure`` a fallback-eligible failure
    (executor_died/timeout/auth/quota; salvage already tried) resolves ONLY the
    attempt row and returns None so the caller can try the next driver.

    ``tcd`` (AGA-1702): the run drives the TCD browser instead of the local Chrome.
    It is recorded as ``tcd_<driver>`` (sharing that driver's rate pool), gets the TCD
    prompt and its own ``TCD_BROWSER_SESSION``, and the bridge releases that lease
    itself however the run ends. The local Chrome recovery after a bad outcome is
    skipped: a TCD-mode run never touched it."""
    strategy = f"tcd_{driver}" if tcd else f"{driver}_bridge"
    if tcd:
        # Created by the tier-3 selection (or _run_tier3 for a claude fallback).
        assert socket_dir is not None
        sock: Path = socket_dir
    attempt_id = bridge.write_ahead(
        request_id=request_id, paper_id=None, run_dir=str(run_dir), strategy=strategy
    )
    executor.write_task_json(run_dir, doi=doi, title=title, start_url=start_url)
    if tcd:
        executor.write_start_url_file(run_dir, start_url)
        prompt = executor.render_tcd_prompt(run_dir)
        session = tcd_browser.session_name(str(request_id), driver)
        tcd_kw: dict[str, Any] = {"env": tcd_browser.session_env(session, sock), "tcd": True}
    else:
        prompt = executor.PROMPT_TEMPLATE.format(run_dir=str(run_dir))
        tcd_kw = {}
    started = _utc_now()
    try:
        if driver == "claude":
            exec_result = await run_claude(
                run_dir, prompt, api_key or "", wall_seconds=claude_wall, **tcd_kw
            )
            # Belt and braces (run_claude already redacts): the key never reaches a DB
            # row, notification or HTTP error detail.
            exec_result.notes = executor.redact_secret(exec_result.notes, api_key)
        else:
            exec_result = await run_codex(
                run_dir, prompt, sandbox_mode or "", wall_seconds=codex_wall, **tcd_kw
            )
        ended = _utc_now()
    except BaseException:
        if tcd:
            # The executor itself was interrupted (its group is already dead: run_* kill
            # on cancel): no result to keep. The close is shielded and re-raises a
            # cancellation that arrives meanwhile.
            await tcd_browser.close_session(session, sock)
        raise
    # The executor returned: whatever happens during the close, that result is
    # persisted below (attempt row, latch) BEFORE a cancellation that arrived during
    # the close is re-raised.
    late_cancel = (
        (await tcd_browser.close_session(session, sock, defer_cancel=True)).cancelled
        if tcd else None
    )
    screenshot = str(run_dir / "final.png") if (run_dir / "final.png").exists() else None
    if exec_result.cost_usd is not None:
        with bridge.db.transaction() as conn:
            limits.record_attempt_cost(conn, attempt_id, exec_result.cost_usd)

    bad_outcome = exec_result.kind in {"timeout", "gave_up", "executor_died", "claude_budget"}
    human_wall = tcd and exec_result.kind == "bot_wall" and bridge.config.human_check_wait_seconds > 0
    if human_wall:
        # Persist the wall before any cancellable human wait, without announcing a
        # terminal failure. The usual sweep can recover this evidence after a crash.
        bridge.resolve_attempt(attempt_id, outcome="bot_wall", message=exec_result.notes or "bot_wall",
                               screenshot_path=screenshot, current_url=exec_result.final_url)
        response = None
    else:
        response = await _resolve_outcome(
            bridge, exec_result=exec_result, request_id=request_id, attempt_id=attempt_id,
            doi=doi, title=title, run_dir=run_dir, window_start=started, window_end=ended,
            screenshot=screenshot, tier_label=strategy,
            defer_failure=defer_failure, deferred=pending,
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
    if late_cancel is not None:
        raise late_cancel  # only now: the outcome above is already persisted

    if human_wall:
        failure = dict(request_id=request_id, attempt_id=attempt_id, screenshot=screenshot,
                       final_url=exec_result.final_url, message=exec_result.notes or "bot_wall")
        if human_check_passed:
            return _finish_failure(bridge, kind="bot_wall", **failure,
                                   wall_reason="the human check came back after his pass")
        url = _preferred_current_url(bridge, request_id, exec_result.final_url) or start_url
        wait = min(bridge.config.human_check_wait_seconds, max(0, int(deadline - MONOTONIC() - 5)))
        result = await tcd_browser.ask_human_check(
            url, reason=_human_check_reason(title), task=_human_check_task(title), wait_seconds=wait)
        if result == "done":
            retried = await _retry_after_human_check(
                bridge, request_id=request_id, run_dir=run_dir, doi=doi, title=title,
                start_url=url, deadline=deadline,
            )
            if retried is not None:
                return retried
        if result in {"done", "pending"}:
            # Store the exact ask URL (not a previous deterministic tier's URL).
            failure["final_url"] = url
            return _finish_failure(bridge, kind="human_check_pending", **failure)
        return _finish_failure(
            bridge, kind="bot_wall", **failure,
            wall_reason="he declined the human check" if result == "declined" else None,
        )

    # chrome recovery on bad outcomes (still holding the lock). After a claude run
    # that overran the request deadline, skip it: KC has already timed out and the
    # lock must be released; the next request's chrome pre-flight recovers.
    # AGA-1702: never after a TCD-mode run (it did not use the local Chrome).
    if (bad_outcome or exec_result.kind in {"auth_error", "claude_auth"}) and not tcd:
        if driver == "claude" and MONOTONIC() >= deadline:
            logger.warning(
                "deferring post-claude chrome recovery to the next request: deadline passed"
            )
            bridge.recovery_pending = True
            return response
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
    deferred: dict[str, Any] | None = None,
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
            if deferred is not None:  # lets the caller finalize it if no fallback runs
                deferred.update(kind=kind, attempt_id=attempt_id, screenshot=screenshot,
                                message=message, final_url=exec_result.final_url)
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
    wall_reason: str | None = None,
) -> Response:
    outcome, event_type, http_status, error_code = _TAXONOMY[kind]
    if kind == "human_check_pending":
        message = _HUMAN_CHECK_PENDING_REASON
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
    if kind in {"bot_wall", "human_check_pending"} or walled_tiers:
        # Preference order for current_url (AGA-596 review round 2, finding 3):
        # codex's own final_url, else the last deterministic tier's wall URL,
        # else the request's own stored inferred_url — codex's OUTCOME_SCHEMA
        # permits a null final_url, and a codex-only wall with no prior
        # deterministic wall must still get a usable URL, not a bare NULL.
        current_url = _preferred_current_url(bridge, request_id, final_url, last_wall_url)
        manual = ManualIntervention(
            reason=(_HUMAN_CHECK_PENDING_REASON if kind == "human_check_pending" else
                    wall_reason or _codex_wall_reason(walled_tiers, kind)),
            screenshot_path=screenshot,
            page_title=None,
            current_url=current_url,
            suggested_next_action=(_HUMAN_CHECK_NEXT_ACTION if kind == "human_check_pending" else
                                   _wall_suggested_action(current_url)),
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
    extra = ({"retry_after_seconds": bridge.config.human_check_retry_seconds}
             if kind == "human_check_pending" else {})
    return _json_error(http_status, error_code, wall_reason or message, request_id=str(request_id),
                       screenshot=screenshot, **extra)


def _redact_all(text: str | None) -> str:
    """Strip every secret the bridge can see (Anthropic / Elsevier keys, the bearer
    token) from text bound for a trial response or log."""
    out = text or ""
    for secret in (executor.anthropic_api_key(), executor.elsevier_api_key(), _configured_token()):
        out = executor.redact_secret(out, secret)
    return out


def _policy_flags(run_dir: Path, *, start_url: str | None = None) -> list[str]:
    """Scan the run dir's model-authored scripts/logs for credential / cookie /
    CAPTCHA-solver patterns. Returns ``"<pattern>@<relative file>"`` entries (never
    file content, which could hold a secret). Reported, not enforced.

    ``start_url`` (AGA-1702, TCD runs only) is the address the bridge wrote to
    ``<run_dir>/start_url.txt``. That one file is skipped while it still holds exactly
    that address: it is request data, like task.json. A local run passes nothing, so a
    file of that name is scanned as on main; so is one the model changed or nested."""
    flags: list[str] = []
    try:
        files = sorted(p for p in run_dir.rglob("*") if p.is_file())
    except OSError:
        return flags
    for path in files:
        rel = path.relative_to(run_dir)
        if start_url is not None and rel == Path(executor.START_URL_FILE):
            expected = start_url.encode("utf-8")
            try:
                # Bounded: the executor can replace this file with one of any size.
                with path.open("rb") as handle:
                    if handle.read(len(expected) + 1) == expected:
                        continue
            except OSError:
                pass
        if (
            path.name in _POLICY_SCAN_SKIP
            or path.name.endswith(".reason.txt")
            or path.suffix.lower() not in _POLICY_SCAN_SUFFIXES
        ):
            continue
        try:
            if path.stat().st_size > _POLICY_SCAN_MAX_BYTES:
                continue
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for name, pattern in _POLICY_PATTERNS:
            if pattern.search(text):
                flags.append(f"{name}@{rel}")
                if len(flags) >= _POLICY_FLAGS_MAX:
                    return flags
    return flags


async def _trial_locked(
    bridge: Bridge, *, doi: str, title: str, driver: str, model: str,
    effort: str | None, max_budget_usd: float | None, browser: str = "local",
) -> Response:
    """One bake-off trial (AGA-1461): the named executor alone, same prompt/task.json/
    chrome pre-flight/wall logic as tier 3, scored by the production acceptance +
    identity gate, but leaving NO production state — nothing is linked into
    papers/paper_links, the dedupe cache is never consulted, and the accepted PDF is
    hashed then deleted. Returns 200 for every outcome that reached a run.

    ``browser="tcd"`` (AGA-1702): the same trial through the TCD browser, recorded as
    ``trial_tcd_<driver>`` (sharing ``trial_<driver>``'s cap): TCD prompt and session,
    no local Chrome pre-flight or recovery, lease released by the bridge."""
    tcd = browser == "tcd"
    strategy = f"trial_tcd_{driver}" if tcd else f"trial_{driver}"
    state = bridge.state_store.load()
    api_key: str | None = None
    if driver == "codex":
        if not state.sandbox_mode:
            return _json_error(503, "selftest_required", "run POST /selftest/codex first")
    else:
        api_key = executor.anthropic_api_key()
        if not _claude_ready(state) or not api_key:
            return _json_error(503, "claude_auth", "claude driver is not available")
    if tcd:
        reason = await _tcd_not_ready_reason(state, driver)
        if reason is not None:
            return _json_error(503, "tcd_unavailable", f"the TCD browser is not ready: {reason}")

    # Rate: the shared GLOBAL LLM spacing (production + trial rows), then the trial's
    # own daily cap. Neither writes a row, so a rejection never starts a clock.
    with bridge.db.transaction() as conn:
        spacing = limits.check_llm_spacing(conn)
        cap = limits.check_rate(conn, strategy)
    if not spacing.allowed:
        return _json_error(429, "cooldown", "an LLM-driven attempt started too recently",
                           retry_after_seconds=spacing.retry_after_seconds)
    if not cap.allowed:
        return _json_error(429, cap.error_code or "daily_cap", "trial daily cap reached")

    if not tcd and not await _chrome_preflight(bridge):
        return _json_error(503, "chrome_down", "Chrome CDP unreachable")

    deadline = MONOTONIC() + REQUEST_BUDGET_SECONDS
    wall = max(1.0, min(float(executor.EXEC_WALL_SECONDS),
                        deadline - MONOTONIC() - CLEANUP_RESERVE_SECONDS))
    trial_id = f"{_utc_now().strftime('%Y%m%dT%H%M%SZ')}_{secrets.token_hex(4)}"
    run_dir = bridge.config.db_path.parent / "trials" / trial_id
    # The trial's OWN download dir: nothing it writes can collide with, or be mistaken
    # for, production files (and nothing in the production dir is ever touched). Created
    # BEFORE the write-ahead row, so a filesystem failure here leaves no running attempt.
    trial_dl = run_dir / "downloads"
    trial_dl.mkdir(parents=True, exist_ok=True)
    if tcd:
        made = tcd_browser.make_socket_dir(run_dir)
        if made is None:
            return _json_error(503, "tcd_unavailable",
                               "no agent-browser socket dir fits the unix socket path limit")
        socket_dir: Path = made
    request_id = bridge.trial_ledger_request_id()
    attempt_id = bridge.write_ahead(
        request_id=request_id, paper_id=None, run_dir=str(run_dir), strategy=strategy
    )
    resolved = False
    try:
        trial_url = f"https://doi.org/{doi}"
        executor.write_task_json(run_dir, doi=doi, title=title, start_url=trial_url)
        if tcd:
            executor.write_start_url_file(run_dir, trial_url)
            prompt = executor.render_tcd_prompt(run_dir, download_dir=trial_dl)
            session = tcd_browser.session_name(f"trial{attempt_id}", driver)
            tcd_kw: dict[str, Any] = {
                "env": tcd_browser.session_env(session, socket_dir), "tcd": True,
            }
        else:
            prompt = executor.render_prompt(run_dir, download_dir=trial_dl)
            tcd_kw = {}
        started = _utc_now()
        try:
            if driver == "claude":
                exec_result = await run_claude(
                    run_dir, prompt, api_key or "", wall_seconds=wall, model=model,
                    max_budget_usd=max_budget_usd, effort=effort, **tcd_kw,
                )
            else:
                exec_result = await run_codex(
                    run_dir, prompt, state.sandbox_mode or "", wall_seconds=wall,
                    model=model, effort=effort, download_dir=trial_dl, **tcd_kw,
                )
            ended = _utc_now()
        except BaseException:
            if tcd:
                await tcd_browser.close_session(session, socket_dir)
            raise
        # A returned result is recorded below even when the trial is cancelled during
        # the close; that cancellation is re-raised only afterwards.
        late_cancel = (
            (await tcd_browser.close_session(session, socket_dir, defer_cancel=True)).cancelled
            if tcd else None
        )
        exec_result.notes = _redact_all(exec_result.notes)
        final_url = _redact_all(exec_result.final_url) or None
        kind = exec_result.kind

        gate, gate_reason, verified_by = "not_run", "", None
        file_sha: str | None = None
        file_size: int | None = None
        salvaged = False
        located: Path | None = None
        if kind == "downloaded":
            located = _locate_downloaded(
                bridge, exec_result.file_path, started, ended, download_dir=trial_dl)
        elif kind in {"timeout", "gave_up", "executor_died", "claude_budget"}:
            # same salvage pass as prod, confined to the trial dir
            located = bridge._locate_in_window(started, ended, download_dir=trial_dl)
            salvaged = located is not None
        if located is not None and bridge._mechanical_gates(located):
            ok, reason = bridge.verify_identity(doi=doi, title=title, path=located)
            gate_reason = reason
            if ok:
                gate = "pass"
                verified_by = "doi" if doi else "title"
                file_sha = bridge._sha256(located)
                file_size = located.stat().st_size
                try:
                    located.unlink()  # metadata only: the PDF is not kept
                except OSError:
                    logger.exception("could not delete the accepted trial PDF")
            else:
                gate = "fail"
                salvaged = False
                _quarantine(located, run_dir=run_dir, reason=reason)
        elif kind == "downloaded":
            gate, gate_reason = "fail", "claimed download not found or failed mechanical gates"
            salvaged = False
        else:
            salvaged = False

        if gate == "pass":
            outcome = "completed"
        elif gate == "fail":
            outcome = "wrong_paper" if located is not None else "no_pdf"
        else:
            outcome = _TAXONOMY.get(kind, (kind,))[0]
        message = "salvaged" if (gate == "pass" and salvaged) else (exec_result.notes or kind)
        bridge.resolve_attempt(
            attempt_id, outcome=outcome, message=message[:300],
            screenshot_path=str(run_dir / "final.png") if (run_dir / "final.png").exists() else None,
            current_url=final_url,
        )
        resolved = True
        if exec_result.cost_usd is not None:
            with bridge.db.transaction() as conn:
                limits.record_attempt_cost(conn, attempt_id, exec_result.cost_usd)

        # Same latch semantics as production for a dead login/key.
        if kind == "auth_error":
            bridge.state_store.update(sandbox_mode=None, last_selftest_ok=False)
        elif kind == "claude_auth":
            bridge.state_store.update(claude_last_selftest_ok=False)
        if late_cancel is not None:
            raise late_cancel  # only now: the outcome above is already recorded

        # Chrome recovery after a bad outcome, exactly like production: skipped past
        # the deadline for claude (latched for the next request's pre-flight).
        bad_outcome = kind in {"timeout", "gave_up", "executor_died", "claude_budget"}
        if (bad_outcome or kind in {"auth_error", "claude_auth"}) and not tcd:
            if driver == "claude" and MONOTONIC() >= deadline:
                logger.warning("deferring post-trial chrome recovery: deadline passed")
                bridge.recovery_pending = True
            else:
                rec = await chrome.recover()
                bridge.state_store.update(last_recovery_action=rec.action)

        tokens = executor.codex_token_total(exec_result.events) if driver == "codex" else None
        body = {
            "trial_id": trial_id,
            "driver": driver,
            "browser": browser,
            "model": model,
            "effort": effort,
            "kind": kind,
            "gate": gate,
            "gate_reason": _redact_all(gate_reason)[:300],
            "verified_by": verified_by,
            "file_sha256": file_sha,
            "file_size": file_size,
            "wall_seconds": round(exec_result.wall_seconds, 2),
            "cost_usd": exec_result.cost_usd if driver == "claude" else None,
            "tokens": tokens,
            "final_url": final_url,
            "notes": _redact_all(exec_result.notes)[:TRIAL_NOTES_MAX],
            "policy_flags": _policy_flags(run_dir, start_url=trial_url if tcd else None),
            "salvaged": bool(gate == "pass" and salvaged),
        }
        return JSONResponse(status_code=200, content=body)
    except BaseException:
        # A crash/cancel after the write-ahead row: close it so it does not sit
        # 'running' until the next restart (it still counts toward spacing + caps). A TCD
        # trial's row already resolved with the run's real outcome is left as it is; a
        # local trial behaves exactly as before (always 'interrupted').
        if not (tcd and resolved):
            try:
                bridge.resolve_attempt(attempt_id, outcome="interrupted", message="trial aborted")
            except Exception:
                logger.exception("could not close the trial attempt row")
        raise


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
