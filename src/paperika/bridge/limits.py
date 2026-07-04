"""Rate discipline + write-ahead attempt bookkeeping for the paperika bridge.

All enforcement is bridge-side and backed by ``paper_attempts`` rows in the live
``papers.db`` (write-ahead accounting, §3). The in-memory lock lives in ``app.py``;
this module owns the *durable* record that makes the lock crash-safe.

Key invariants (see docs/bridge_contracts.md):
- Spacing + the daily cap count ``paper_attempts`` rows by ``started_at`` of ANY
  non-null ``outcome`` (``running`` and ``interrupted`` included). Counting at
  *launch* means failures and crashes spend budget too — deliberate.
- Dedupe cache hits and pre-flight rejections write NO attempt row, so they never
  start the spacing clock or the daily cap.
- The new columns (``started_at``/``finished_at``/``outcome``/``run_dir``) are
  added by a guarded idempotent migration (also re-run at bridge startup).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import sqlite3

MIN_SPACING_SECONDS = 180
DAILY_CAP = 20

# Per-strategy rate discipline for the tiered ladder (AGA-339): each entry is
# ``(min_spacing_seconds, daily_cap)``. The cheap deterministic tiers get tighter
# spacing and a higher daily cap than the expensive ~117s codex session. The
# ``codex_bridge`` entry mirrors the module-level MIN_SPACING_SECONDS/DAILY_CAP,
# which are retained as the codex defaults for backward compatibility.
RATE_RULES: dict[str, tuple[int, int]] = {
    "direct_fetch": (30, 100),
    "scripted_browser": (90, 40),
    "codex_bridge": (MIN_SPACING_SECONDS, DAILY_CAP),
}

# Columns the bridge adds to paper_attempts (request_id already exists in the
# live schema). Guarded idempotent ALTERs — safe to run on every startup.
_BRIDGE_ATTEMPT_COLUMNS: tuple[tuple[str, str], ...] = (
    ("started_at", "TEXT"),
    ("finished_at", "TEXT"),
    ("outcome", "TEXT"),
    ("run_dir", "TEXT"),
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.isoformat()


def ensure_bridge_columns(conn: sqlite3.Connection) -> None:
    """Add the bridge's paper_attempts columns if missing. Idempotent."""
    existing = {row[1] for row in conn.execute("PRAGMA table_info(paper_attempts)")}
    for name, sql_type in _BRIDGE_ATTEMPT_COLUMNS:
        if name not in existing:
            conn.execute(f"ALTER TABLE paper_attempts ADD COLUMN {name} {sql_type}")
    conn.commit()


@dataclass(slots=True)
class RateDecision:
    allowed: bool
    error_code: str | None = None  # "cooldown" | "daily_cap"
    retry_after_seconds: int | None = None


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def most_recent_attempt_start(
    conn: sqlite3.Connection, *, strategy: str | None = None
) -> datetime | None:
    """started_at of the most recent attempt of ANY recorded outcome. When
    ``strategy`` is given, only that strategy's attempts count (per-strategy
    spacing); ``None`` considers every strategy."""
    if strategy is None:
        row = conn.execute(
            "SELECT started_at FROM paper_attempts "
            "WHERE started_at IS NOT NULL AND outcome IS NOT NULL "
            "ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT started_at FROM paper_attempts "
            "WHERE started_at IS NOT NULL AND outcome IS NOT NULL AND strategy = ? "
            "ORDER BY started_at DESC LIMIT 1",
            (strategy,),
        ).fetchone()
    return _parse_iso(row[0]) if row else None


def attempts_today(
    conn: sqlite3.Connection,
    strategy: str | None = None,
    *,
    now: datetime | None = None,
) -> int:
    """Count attempts launched in the current UTC day (any outcome). ``strategy``
    ``None`` counts every strategy (the healthz total); a string scopes the count to
    that strategy's per-strategy daily cap."""
    now = now or utc_now()
    day_start = now.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    if strategy is None:
        row = conn.execute(
            "SELECT COUNT(*) FROM paper_attempts "
            "WHERE started_at IS NOT NULL AND outcome IS NOT NULL AND started_at >= ?",
            (iso(day_start),),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT COUNT(*) FROM paper_attempts "
            "WHERE started_at IS NOT NULL AND outcome IS NOT NULL AND started_at >= ? "
            "AND strategy = ?",
            (iso(day_start), strategy),
        ).fetchone()
    return int(row[0]) if row else 0


def check_rate(
    conn: sqlite3.Connection,
    strategy: str = "codex_bridge",
    *,
    now: datetime | None = None,
    min_spacing_seconds: int | None = None,
    daily_cap: int | None = None,
) -> RateDecision:
    """Per-strategy pre-flight spacing + daily-cap gate. Consults ONLY attempts of
    ``strategy``. Called inside the lock, BEFORE the write-ahead row is inserted (so
    a rejection never starts the clock). ``min_spacing_seconds``/``daily_cap`` fall
    back to that strategy's ``RATE_RULES`` entry when not explicitly overridden."""
    now = now or utc_now()
    default_spacing, default_cap = RATE_RULES.get(strategy, (MIN_SPACING_SECONDS, DAILY_CAP))
    if min_spacing_seconds is None:
        min_spacing_seconds = default_spacing
    if daily_cap is None:
        daily_cap = default_cap

    last = most_recent_attempt_start(conn, strategy=strategy)
    if last is not None:
        elapsed = (now - last).total_seconds()
        if elapsed < min_spacing_seconds:
            retry_after = int(min_spacing_seconds - elapsed) + 1
            return RateDecision(False, "cooldown", retry_after)

    if attempts_today(conn, strategy, now=now) >= daily_cap:
        return RateDecision(False, "daily_cap")

    return RateDecision(True)


def _next_attempt_number(conn: sqlite3.Connection, request_id: int) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(attempt_number), 0) FROM paper_attempts WHERE request_id = ?",
        (request_id,),
    ).fetchone()
    return int(row[0]) + 1 if row else 1


def write_ahead_attempt(
    conn: sqlite3.Connection,
    *,
    request_id: int,
    paper_id: int | None,
    run_dir: str,
    strategy: str = "codex_bridge",
    now: datetime | None = None,
) -> int:
    """Insert the write-ahead paper_attempts row (outcome='running') BEFORE codex
    spawns. From this instant the row counts toward spacing + the daily cap.

    ``status`` (pre-existing NOT NULL column) is mirrored to 'running'; the bridge
    taxonomy lives in the new ``outcome`` column.
    """
    now = now or utc_now()
    started = iso(now)
    attempt_number = _next_attempt_number(conn, request_id)
    cursor = conn.execute(
        """
        INSERT INTO paper_attempts (
            request_id, paper_id, attempt_number, status, strategy,
            created_at, started_at, outcome, run_dir
        ) VALUES (?, ?, ?, 'running', ?, ?, ?, 'running', ?)
        """,
        (request_id, paper_id, attempt_number, strategy, started, started, run_dir),
    )
    conn.commit()
    return int(cursor.lastrowid)


# status mirror for the NOT-NULL paper_attempts.status column, by outcome.
_SUCCESS_OUTCOMES = {"completed"}


def resolve_attempt(
    conn: sqlite3.Connection,
    attempt_id: int,
    *,
    outcome: str,
    paper_id: int | None = None,
    message: str | None = None,
    screenshot_path: str | None = None,
    now: datetime | None = None,
) -> None:
    """UPDATE the write-ahead row with its final outcome + finished_at."""
    now = now or utc_now()
    status = "downloaded" if outcome in _SUCCESS_OUTCOMES else "failed"
    conn.execute(
        """
        UPDATE paper_attempts
        SET outcome = ?, status = ?, finished_at = ?, message = COALESCE(?, message),
            paper_id = COALESCE(?, paper_id),
            screenshot_path = COALESCE(?, screenshot_path)
        WHERE id = ?
        """,
        (outcome, status, iso(now), message, paper_id, screenshot_path, attempt_id),
    )
    conn.commit()


def running_attempts(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """All attempt rows still outcome='running' (startup-sweep input)."""
    conn.row_factory = sqlite3.Row
    return conn.execute(
        "SELECT * FROM paper_attempts WHERE outcome = 'running' ORDER BY id ASC"
    ).fetchall()
