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
import os
from pathlib import Path
import re
import subprocess
from typing import Any
from urllib.parse import urlparse

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

from ..config import PaperikaConfig
from ..db import Database, normalize_title
from ..downloader import Downloader
from ..models import LocateCandidate, ParsedInput
from ..notifications import NotificationEvent, emit_notification_event
from . import chrome, executor, limits

# --- constants -------------------------------------------------------------

MAX_FILE_SIZE = 500 * 1024 * 1024  # mirror KC citations/service.py MAX_FILE_SIZE
TOKEN_ENV = "PAPERIKA_BRIDGE_TOKEN"
DOI_RE = re.compile(r"^10\.\d{4,9}/\S+$")
TITLE_MAX = 300
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")
SALVAGE_GRACE_SECONDS = 10
SWEEP_WINDOW_SECONDS = executor.EXEC_WALL_SECONDS + 30


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
    "gave_up": ("executor_gave_up", "executor_failed", 502, "codex_gave_up"),
    "auth_error": ("auth_error", "executor_failed", 503, "codex_auth"),
    "timeout": ("timeout", "executor_failed", 504, "executor_timeout"),
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

    def write_ahead(self, *, request_id: int, paper_id: int | None, run_dir: str) -> int:
        with self.db.transaction() as conn:
            return limits.write_ahead_attempt(conn, request_id=request_id, paper_id=paper_id, run_dir=run_dir)

    def resolve_attempt(self, attempt_id: int, **kwargs: Any) -> None:
        with self.db.transaction() as conn:
            limits.resolve_attempt(conn, attempt_id, **kwargs)


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
    """On every start: salvage-first over stale outcome='running' rows (§2.3)."""
    with bridge.db.transaction() as conn:
        rows = limits.running_attempts(conn)
    for row in rows:
        request_id = row["request_id"]
        started = row["started_at"]
        run_dir = row["run_dir"]
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
            try:
                bridge.db.update_request_status(request_id, "failed")
            except KeyError:
                pass
            bridge.emit(
                event_type="executor_failed", request_id=request_id, paper_id=row["paper_id"],
                status_after="failed", status_before="in_progress",
                message="bridge restarted mid-run (subtype=interrupted)",
            )


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


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
        status = "ok" if result.ok else "degraded"
        http_status = 200 if result.ok else 503

        # Unauthenticated tier: liveness only.
        header = request.headers.get("authorization", "")
        presented = header[7:] if header.lower().startswith("bearer ") else ""
        authed = bool(token_configured and presented and hmac.compare_digest(presented, _configured_token()))
        if not authed:
            return JSONResponse(
                status_code=http_status,
                content={"status": status, "token_configured": token_configured},
            )

        state = bridge.state_store.load()
        with bridge.db.transaction() as conn:
            downloads_today = limits.attempts_today(conn)
        body = {
            "status": status,
            "chrome": {
                "cdp_connectable": result.ok,
                "browser": result.version,
                "probe_age_seconds": int(result.age_seconds() or 0),
            },
            "codex": {
                "auth_json_present": Path("/home/agastya/.codex/auth.json").exists(),
                "last_selftest_ok": state.last_selftest_ok,
                "last_selftest_at": state.last_selftest_at,
                "sandbox_mode": state.sandbox_mode,
            },
            "limits": {
                "active_request": "locked" if bridge.lock.locked() else None,
                "downloads_today": downloads_today,
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
            },
        )

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
            return await _download_locked(bridge, doi=doi, title=title, start_url=start_url,
                                          requested_by=payload.requested_by)

    return app


async def _download_locked(
    bridge: Bridge, *, doi: str, title: str, start_url: str, requested_by: str
) -> Response:
    # 4. rate pre-flight
    with bridge.db.transaction() as conn:
        decision = limits.check_rate(conn)
    if not decision.allowed:
        if decision.error_code == "cooldown":
            return _json_error(429, "cooldown", "minimum spacing not elapsed",
                               retry_after_seconds=decision.retry_after_seconds)
        return _json_error(429, "daily_cap", "daily download cap reached")

    # 5½. sandbox gate — refuse before any attempt row if no persisted mode
    state = bridge.state_store.load()
    if not state.sandbox_mode:
        return _json_error(503, "selftest_required", "run POST /selftest/codex first")

    # 5. chrome pre-flight (fresh ws-connect probe) + idle recycle
    await chrome.idle_recycle_if_stale()
    result = await chrome.probe(fresh=True)
    if not result.ok:
        await chrome.recover()
        result = await chrome.probe(fresh=True)
        if not result.ok:
            return _json_error(503, "chrome_down", "Chrome CDP unreachable")

    # 6. write-ahead bookkeeping (BEFORE codex spawns)
    request_id = bridge.db.create_request(
        raw_input=doi, inferred_title=title or None, inferred_doi=doi,
        inferred_url=start_url, paper_id=None,
    )
    bridge.db.update_request_status(request_id, "in_progress")
    run_dir = bridge.config.db_path.parent / "codex_runs" / str(request_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    attempt_id = bridge.write_ahead(request_id=request_id, paper_id=None, run_dir=str(run_dir))

    # 7. codex executor
    executor.write_task_json(run_dir, doi=doi, title=title, start_url=start_url)
    prompt = executor.PROMPT_TEMPLATE.format(run_dir=str(run_dir))
    started = _utc_now()
    exec_result = await executor.run_codex(run_dir, prompt, state.sandbox_mode)
    ended = _utc_now()
    screenshot = str(run_dir / "final.png") if (run_dir / "final.png").exists() else None

    # 8. locate + verify (downloaded path AND salvage on bad outcomes)
    bad_outcome = exec_result.kind in {"timeout", "gave_up"}
    response = await _resolve_outcome(
        bridge, exec_result=exec_result, request_id=request_id, attempt_id=attempt_id,
        doi=doi, title=title, run_dir=run_dir, window_start=started, window_end=ended,
        screenshot=screenshot,
    )

    # 10. chrome recovery on bad outcomes (still holding the lock)
    if bad_outcome or exec_result.kind == "auth_error":
        rec = await chrome.recover()
        bridge.state_store.update(last_recovery_action=rec.action)

    return response


async def _resolve_outcome(
    bridge: Bridge, *, exec_result: executor.ExecResult, request_id: int, attempt_id: int,
    doi: str, title: str, run_dir: Path, window_start: datetime, window_end: datetime,
    screenshot: str | None,
) -> Response:
    kind = exec_result.kind

    # downloaded path
    if kind == "downloaded":
        located = None
        if exec_result.file_path:
            p = Path(exec_result.file_path)
            if p.exists():
                located = p
        if located is None:
            located = bridge._locate_in_window(window_start, window_end)
        verify = await _verify_and_finish(
            bridge, located=located, doi=doi, title=title, request_id=request_id,
            attempt_id=attempt_id, final_url=exec_result.final_url, screenshot=screenshot,
            salvaged=False, run_dir=run_dir,
        )
        if verify is not None:
            return verify
        # downloaded but verify failed ⇒ wrong_paper
        return _finish_failure(bridge, kind="wrong_paper", request_id=request_id,
                               attempt_id=attempt_id, screenshot=screenshot,
                               message="downloaded PDF failed identity verification")

    # salvage pass on timeout/gave_up
    if kind in {"timeout", "gave_up"}:
        located = bridge._locate_in_window(window_start, window_end)
        if located is not None and bridge._mechanical_gates(located):
            ok, _reason = bridge.verify_identity(doi=doi, title=title, path=located)
            if ok:
                salvage = await _verify_and_finish(
                    bridge, located=located, doi=doi, title=title, request_id=request_id,
                    attempt_id=attempt_id, final_url=exec_result.final_url, screenshot=screenshot,
                    salvaged=True, run_dir=run_dir,
                )
                if salvage is not None:
                    return salvage
        # salvage found nothing valid ⇒ original failure taxonomy
        return _finish_failure(bridge, kind=kind, request_id=request_id, attempt_id=attempt_id,
                               screenshot=screenshot, message=exec_result.notes or kind)

    # terminal failures (throttled/paywalled/bot_wall/gave_up-with-no-file/auth)
    return _finish_failure(bridge, kind=kind, request_id=request_id, attempt_id=attempt_id,
                           screenshot=screenshot, message=exec_result.notes or kind)


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
    run_dir: Path,
) -> Response | None:
    """Run mechanical + identity gates; on pass, link + resolve + stream bytes.
    Returns None when no valid file (caller decides wrong_paper vs original)."""
    if located is None or not bridge._mechanical_gates(located):
        return None
    ok, reason = bridge.verify_identity(doi=doi, title=title, path=located)
    if not ok:
        # Quarantine the candidate (downloaded path ⇒ wrong_paper; salvage ⇒ keep
        # the original failure). Either way move it out of the dedupe dir. The
        # caller resolves the attempt outcome (no double-resolve here).
        _quarantine(located, run_dir=Path(run_dir), reason=reason)
        return None

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
    }
    if salvaged:
        headers["X-Paperika-Salvaged"] = "true"
    return Response(content=data, media_type="application/pdf", headers=headers)


def _finish_failure(
    bridge: Bridge, *, kind: str, request_id: int, attempt_id: int,
    screenshot: str | None, message: str,
) -> Response:
    outcome, event_type, http_status, error_code = _TAXONOMY[kind]
    bridge.resolve_attempt(attempt_id, outcome=outcome, message=message, screenshot_path=screenshot)
    try:
        bridge.db.update_request_status(request_id, "failed")
    except KeyError:
        pass
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
