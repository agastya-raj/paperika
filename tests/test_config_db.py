from pathlib import Path
import sqlite3

from paperika.config import PaperikaConfig
from paperika.db import Database
from paperika.models import ManualIntervention


def test_init_db_creates_expected_tables(tmp_path: Path):
    config = PaperikaConfig(
        db_path=tmp_path / "papers.db",
        download_dir=tmp_path / "downloads",
        screenshot_dir=tmp_path / "shots",
    )
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()

    conn = sqlite3.connect(config.db_path)
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    names = {row[0] for row in rows}
    assert {"papers", "paper_links", "paper_requests", "paper_attempts"}.issubset(names)


def _make_db(tmp_path: Path) -> Database:
    config = PaperikaConfig(
        db_path=tmp_path / "papers.db",
        download_dir=tmp_path / "downloads",
        screenshot_dir=tmp_path / "shots",
    )
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    return db


# ------------------------------------------- AGA-596 P5: manual_* COALESCE ---


def test_update_request_status_preserves_manual_fields_when_not_supplied(tmp_path: Path):
    """A status transition that passes manual=None must NOT erase a previously
    recorded operator handoff — only a transition that itself supplies a fresh
    ManualIntervention may overwrite those columns. Before this fix the UPDATE had
    no COALESCE on manual_*, so ANY later status transition nulled the handoff."""
    db = _make_db(tmp_path)
    request_id = db.create_request(
        raw_input="10.1000/manual-persist", inferred_title="Manual Persist Paper",
        inferred_doi="10.1000/manual-persist", inferred_url=None, paper_id=None,
    )
    db.update_request_status(
        request_id, "failed",
        manual=ManualIntervention(
            reason="bot wall", screenshot_path="/tmp/wall.png",
            page_title="Verify you are human", current_url="https://pub.example/wall",
            suggested_next_action="Open the URL yourself.",
        ),
    )
    # A later, unrelated status update supplies no ManualIntervention (manual=None
    # default) — e.g. any subsequent bridge/queue transition on this request.
    db.update_request_status(request_id, "failed")

    row = db.get_request(request_id)
    assert row["manual_reason"] == "bot wall"
    assert row["manual_screenshot_path"] == "/tmp/wall.png"
    assert row["manual_page_title"] == "Verify you are human"
    assert row["manual_current_url"] == "https://pub.example/wall"
    assert row["manual_suggested_next_action"] == "Open the URL yourself."


def test_update_request_status_overwrites_manual_fields_when_supplied(tmp_path: Path):
    """A later transition that DOES supply a fresh ManualIntervention replaces the
    stale one — the fix must not make manual_* permanently sticky."""
    db = _make_db(tmp_path)
    request_id = db.create_request(
        raw_input="10.1000/manual-overwrite", inferred_title="Manual Overwrite Paper",
        inferred_doi="10.1000/manual-overwrite", inferred_url=None, paper_id=None,
    )
    db.update_request_status(
        request_id, "failed",
        manual=ManualIntervention(reason="first wall", screenshot_path="/tmp/first.png"),
    )
    db.update_request_status(
        request_id, "failed",
        manual=ManualIntervention(reason="second wall", screenshot_path="/tmp/second.png"),
    )
    row = db.get_request(request_id)
    assert row["manual_reason"] == "second wall"
    assert row["manual_screenshot_path"] == "/tmp/second.png"


def test_update_request_status_manual_stays_null_when_never_set(tmp_path: Path):
    """A request that never had a ManualIntervention keeps manual_* NULL through
    ordinary status transitions (no false-positive handoff)."""
    db = _make_db(tmp_path)
    request_id = db.create_request(
        raw_input="10.1000/manual-never", inferred_title="Never Manual Paper",
        inferred_doi="10.1000/manual-never", inferred_url=None, paper_id=None,
    )
    db.update_request_status(request_id, "in_progress")
    db.update_request_status(request_id, "completed")
    row = db.get_request(request_id)
    assert row["manual_reason"] is None
    assert row["manual_screenshot_path"] is None


def test_update_request_status_clears_manual_fields_on_success(tmp_path: Path):
    """A later transition to a SUCCESS status must clear manual_* unconditionally —
    a completed request must never keep displaying a stale CAPTCHA-wall handoff,
    even though ordinary (non-success) transitions preserve it (contrast with
    test_update_request_status_preserves_manual_fields_when_not_supplied)."""
    db = _make_db(tmp_path)
    request_id = db.create_request(
        raw_input="10.1000/manual-clear-on-success", inferred_title="Manual Clear On Success Paper",
        inferred_doi="10.1000/manual-clear-on-success", inferred_url=None, paper_id=None,
    )
    db.update_request_status(
        request_id, "failed",
        manual=ManualIntervention(
            reason="bot wall", screenshot_path="/tmp/wall.png",
            page_title="Verify you are human", current_url="https://pub.example/wall",
            suggested_next_action="Open the URL yourself.",
        ),
    )
    db.update_request_status(request_id, "completed")

    row = db.get_request(request_id)
    assert row["status"] == "completed"
    assert row["manual_reason"] is None
    assert row["manual_screenshot_path"] is None
    assert row["manual_page_title"] is None
    assert row["manual_current_url"] is None
    assert row["manual_suggested_next_action"] is None


def test_update_request_status_clears_manual_fields_for_every_success_alias(tmp_path: Path):
    """All three success spellings used across the codebase — the bridge's
    "completed"; downloader.py's "downloaded" and "completed_deduped" — must each
    clear a prior manual_* handoff."""
    db = _make_db(tmp_path)
    for success_status in ("downloaded", "completed_deduped", "completed"):
        request_id = db.create_request(
            raw_input=f"10.1000/manual-clear-{success_status}", inferred_title="t",
            inferred_doi=f"10.1000/manual-clear-{success_status}", inferred_url=None, paper_id=None,
        )
        db.update_request_status(request_id, "failed", manual=ManualIntervention(reason="bot wall"))
        db.update_request_status(request_id, success_status)
        row = db.get_request(request_id)
        assert row["manual_reason"] is None, f"{success_status} should clear manual_reason"
