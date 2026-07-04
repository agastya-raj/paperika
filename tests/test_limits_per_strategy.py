"""AGA-339 per-strategy rate discipline (bridge/limits.py).

The tiered ladder rate-limits each strategy independently: spacing + daily cap are
scoped to that strategy's own paper_attempts rows. These tests drive limits.py
directly against a migrated temp papers.db (no network, no app).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3

import pytest

from paperika.bridge import app as bridge_app
from paperika.bridge import limits
from paperika.config import PaperikaConfig


def _now() -> datetime:
    return datetime.now(timezone.utc)


@pytest.fixture
def conn(tmp_path: Path) -> sqlite3.Connection:
    cfg = PaperikaConfig(
        db_path=tmp_path / "papers.db",
        download_dir=tmp_path / "downloads",
        screenshot_dir=tmp_path / "shots",
        notification_dir=tmp_path / "events",
    )
    bridge_app.build_bridge(cfg)  # init + idempotent bridge-column migration
    c = sqlite3.connect(cfg.db_path)
    c.row_factory = sqlite3.Row
    c.execute(
        "INSERT INTO paper_requests (raw_input, status, created_at, updated_at) VALUES ('x','in_progress',?,?)",
        (limits.iso(_now()), limits.iso(_now())),
    )
    c.commit()
    yield c
    c.close()


def _seed(c: sqlite3.Connection, strategy: str, started: datetime, *, count: int = 1) -> None:
    rid = c.execute("SELECT id FROM paper_requests LIMIT 1").fetchone()[0]
    ts = limits.iso(started)
    for _ in range(count):
        c.execute(
            "INSERT INTO paper_attempts (request_id, attempt_number, status, strategy, created_at, started_at, outcome) "
            "VALUES (?, (SELECT COALESCE(MAX(attempt_number), 0) + 1 FROM paper_attempts), 'failed', ?, ?, ?, 'gave_up')",
            (rid, strategy, ts, ts),
        )
    c.commit()


def test_rate_rules_values():
    assert limits.RATE_RULES == {
        "direct_fetch": (30, 100),
        "scripted_browser": (90, 40),
        "codex_bridge": (180, 20),
    }
    # module-level codex defaults are retained for backward compat.
    assert (limits.MIN_SPACING_SECONDS, limits.DAILY_CAP) == limits.RATE_RULES["codex_bridge"]


def test_check_rate_uses_each_strategy_spacing(conn):
    # 45s ago: past direct_fetch's 30s window, still inside scripted (90s) + codex (180s).
    _seed(conn, "direct_fetch", _now() - timedelta(seconds=45))
    _seed(conn, "scripted_browser", _now() - timedelta(seconds=45))
    _seed(conn, "codex_bridge", _now() - timedelta(seconds=45))
    assert limits.check_rate(conn, "direct_fetch").allowed is True
    scripted = limits.check_rate(conn, "scripted_browser")
    assert scripted.allowed is False and scripted.error_code == "cooldown"
    codex = limits.check_rate(conn, "codex_bridge")
    assert codex.allowed is False and codex.error_code == "cooldown"


def test_spacing_isolation_across_strategies(conn):
    # a very recent codex attempt must not block direct_fetch (independent clocks).
    _seed(conn, "codex_bridge", _now() - timedelta(seconds=1))
    assert limits.check_rate(conn, "direct_fetch").allowed is True
    codex = limits.check_rate(conn, "codex_bridge")
    assert codex.allowed is False and codex.error_code == "cooldown"


def test_check_rate_per_strategy_daily_cap(conn):
    # 100 direct_fetch attempts today ⇒ direct_fetch daily-capped; others untouched.
    _seed(conn, "direct_fetch", _now() - timedelta(hours=1), count=100)
    capped = limits.check_rate(conn, "direct_fetch", min_spacing_seconds=0)
    assert capped.allowed is False and capped.error_code == "daily_cap"
    assert limits.check_rate(conn, "scripted_browser", min_spacing_seconds=0).allowed is True


def test_attempts_today_strategy_filter(conn):
    _seed(conn, "direct_fetch", _now() - timedelta(minutes=5), count=3)
    _seed(conn, "scripted_browser", _now() - timedelta(minutes=5), count=2)
    _seed(conn, "codex_bridge", _now() - timedelta(minutes=5), count=1)
    assert limits.attempts_today(conn) == 6  # None ⇒ every strategy
    assert limits.attempts_today(conn, "direct_fetch") == 3
    assert limits.attempts_today(conn, "scripted_browser") == 2
    assert limits.attempts_today(conn, "codex_bridge") == 1


def test_check_rate_defaults_to_codex_bridge(conn):
    # bare check_rate(conn) keeps the legacy codex-strategy behavior (backward compat).
    _seed(conn, "codex_bridge", _now() - timedelta(seconds=10))
    d = limits.check_rate(conn)
    assert d.allowed is False and d.error_code == "cooldown"
    # ...and it consults ONLY codex_bridge rows: direct_fetch (no rows) stays allowed.
    assert limits.check_rate(conn, "direct_fetch").allowed is True
