from pathlib import Path
import sqlite3

from paperika.config import PaperikaConfig
from paperika.db import Database
from paperika.models import LocateCandidate, ManualIntervention


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


def test_connect_recreates_schema_when_db_file_missing_or_empty(tmp_path: Path):
    """AGA-1250: a DB deleted underneath a running process must not leave every
    later connect() on a table-less file."""
    config = PaperikaConfig(db_path=tmp_path / "state" / "papers.db", download_dir=tmp_path / "dl")
    db = Database.from_config(config)
    db.init()
    config.db_path.unlink()
    with db.connect() as conn:
        assert conn.execute("SELECT count(*) FROM papers").fetchone()[0] == 0
    config.db_path.write_bytes(b"")
    with db.connect() as conn:
        assert conn.execute("SELECT count(*) FROM paper_attempts").fetchone()[0] == 0
    import shutil
    shutil.rmtree(config.db_path.parent)
    with db.connect() as conn:
        assert conn.execute("SELECT count(*) FROM paper_requests").fetchone()[0] == 0


def test_default_state_paths_are_not_under_hermes_home():
    """AGA-1250: ~/.hermes belongs to the Hermes agent and gets wiped by its
    updates; paperika state must not live there."""
    config = PaperikaConfig()
    for path in (config.db_path, config.screenshot_dir, config.notification_dir):
        assert ".hermes" not in path.parts, path


_PRE_REVISION_SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE papers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT, normalized_title TEXT, doi TEXT UNIQUE,
    status TEXT NOT NULL DEFAULT 'new', local_pdf_path TEXT,
    verified_pdf INTEGER NOT NULL DEFAULT 0, canonical_url TEXT,
    open_access_url TEXT, year INTEGER, venue TEXT, authors_json TEXT,
    abstract TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);

CREATE TABLE paper_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_input TEXT NOT NULL,
    paper_id INTEGER REFERENCES papers(id) ON DELETE SET NULL,
    inferred_title TEXT, inferred_doi TEXT, inferred_url TEXT,
    source_mode TEXT NOT NULL DEFAULT 'local_download',
    status TEXT NOT NULL DEFAULT 'queued', priority INTEGER NOT NULL DEFAULT 100,
    force_redownload INTEGER NOT NULL DEFAULT 0, next_retry_at TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    manual_reason TEXT, manual_screenshot_path TEXT, manual_page_title TEXT,
    manual_current_url TEXT, manual_suggested_next_action TEXT,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);

CREATE TABLE paper_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id INTEGER NOT NULL REFERENCES paper_requests(id) ON DELETE CASCADE,
    paper_id INTEGER REFERENCES papers(id) ON DELETE SET NULL,
    attempt_number INTEGER NOT NULL, status TEXT NOT NULL, strategy TEXT,
    message TEXT, screenshot_path TEXT, page_title TEXT, current_url TEXT,
    created_at TEXT NOT NULL
);
"""


def test_init_migrates_pre_revision_schema_idempotently(tmp_path: Path):
    """AGA-596 review round 5, finding 3: pin migration deletion-sensitivity —
    removing db._ensure_request_revision_column() from Database.init() would
    leave this test failing (an existing deployed database, like the gpu-box
    one, has paper_requests WITHOUT a revision column) while every OTHER test
    stays green (they all start from the current, already-migrated schema).
    Builds a database from the pre-revision schema (everything current EXCEPT
    the revision column), runs init() TWICE (idempotence — a second bridge
    restart or CLI invocation must not error or reset existing data), then
    asserts the column was backfilled to 0 for a pre-existing row and CAS
    works against it immediately."""
    config = PaperikaConfig(
        db_path=tmp_path / "papers.db",
        download_dir=tmp_path / "downloads",
        screenshot_dir=tmp_path / "shots",
    )
    config.ensure_runtime_dirs()

    conn = sqlite3.connect(config.db_path)
    conn.executescript(_PRE_REVISION_SCHEMA)
    conn.execute(
        "INSERT INTO paper_requests (raw_input, status, created_at, updated_at) "
        "VALUES ('10.1000/pre-revision', 'downloaded', 't', 't')"
    )
    conn.commit()
    conn.close()

    db = Database.from_config(config)
    db.init()
    db.init()  # idempotence: a second init() (e.g. a later bridge restart) must not error

    conn = sqlite3.connect(config.db_path)
    conn.row_factory = sqlite3.Row
    columns = {row[1] for row in conn.execute("PRAGMA table_info(paper_requests)")}
    assert "revision" in columns
    row = conn.execute("SELECT * FROM paper_requests WHERE raw_input = '10.1000/pre-revision'").fetchone()
    assert row["revision"] == 0  # backfilled, not NULL
    conn.close()

    applied = db.update_request_status(row["id"], "failed", expected_revision=row["revision"])
    assert applied is True
    updated = db.get_request(row["id"])
    assert updated["status"] == "failed"
    assert updated["revision"] == 1


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


# --------------------------------- AGA-596 review finding 3: stale-failure race ---


def test_update_request_status_stale_failure_after_success_does_not_overwrite(tmp_path: Path):
    """A slower/stale failure transition committing AFTER a success has already
    landed (e.g. two workers race on the same retryable request) must not
    resurrect the request as failed nor repopulate manual_* — the WHERE-clause
    guard blocks any non-success status update once the row is already in a
    _SUCCESS_STATUSES state. Sequential commits are enough to pin this: the guard
    is about commit ORDER, not true concurrent access."""
    db = _make_db(tmp_path)
    paper_id = db.upsert_located_paper(LocateCandidate(title="Race Stale Failure Paper"))
    request_id = db.create_request(
        raw_input="10.1000/race-stale-failure", inferred_title="Race Stale Failure Paper",
        inferred_doi="10.1000/race-stale-failure", inferred_url=None, paper_id=None,
    )
    db.update_request_status(request_id, "completed", paper_id=paper_id)
    # A stale failure (e.g. a slower worker's outcome) commits after the success.
    # The return value must make the guarded no-op detectable (review round-2
    # finding 2) — a caller emitting a status-change notification needs this to
    # know it must skip the emit.
    applied = db.update_request_status(
        request_id, "failed",
        manual=ManualIntervention(reason="stale bot wall", screenshot_path="/tmp/stale.png"),
    )
    assert applied is False
    row = db.get_request(request_id)
    assert row["status"] == "completed"
    assert row["paper_id"] == paper_id
    assert row["manual_reason"] is None
    assert row["manual_screenshot_path"] is None


def test_update_request_status_success_after_success_still_applies(tmp_path: Path):
    """The stale-failure guard blocks only a NON-success transition landing after
    a success — a later transition that is ITSELF a success status (e.g. a
    redundant/idempotent completion) must still apply."""
    db = _make_db(tmp_path)
    paper_a = db.upsert_located_paper(LocateCandidate(title="Race Success Success Paper A"))
    paper_b = db.upsert_located_paper(LocateCandidate(title="Race Success Success Paper B"))
    request_id = db.create_request(
        raw_input="10.1000/race-success-success", inferred_title="t",
        inferred_doi="10.1000/race-success-success", inferred_url=None, paper_id=None,
    )
    db.update_request_status(request_id, "completed", paper_id=paper_a)
    applied = db.update_request_status(request_id, "completed", paper_id=paper_b)
    assert applied is True
    row = db.get_request(request_id)
    assert row["paper_id"] == paper_b


def test_update_request_status_compare_and_set_allows_legitimate_reopen(tmp_path: Path):
    """MEDIUM review round-3 finding 1: the guard's purpose is only "a stale
    failure must not overwrite a success", not "success is forever". A caller
    that supplies expected_revision matching the row's ACTUAL current revision
    (compare-and-set — not a bare caller-asserted allow_reopen=True, which
    review round 2's fix used and round 3 found unconditionally bypassed the
    guard for every caller, race or not) can still transition an
    already-successful request to a non-success status — this is how
    downloader.py's legitimate force_redownload retry works."""
    db = _make_db(tmp_path)
    request_id = db.create_request(
        raw_input="10.1000/legit-reopen", inferred_title="t",
        inferred_doi="10.1000/legit-reopen", inferred_url=None, paper_id=None,
    )
    db.update_request_status(request_id, "completed")
    revision_after_success = db.get_request(request_id)["revision"]
    blocked = db.update_request_status(request_id, "failed")
    assert blocked is False
    applied = db.update_request_status(request_id, "failed", expected_revision=revision_after_success)
    assert applied is True
    row = db.get_request(request_id)
    assert row["status"] == "failed"


def test_update_request_status_compare_and_set_blocks_stale_worker_after_success(tmp_path: Path):
    """HIGH review round-3 finding 1: two workers read the same request at
    status='queued'. The FASTER worker succeeds first (status -> 'downloaded').
    The SLOWER worker's failure write, using the revision it read before doing
    its work, must no-op — CAS correctly detects the row moved on, unlike the
    previous unconditional allow_reopen=True (round 2's fix) which would have
    resurrected it regardless of any race."""
    db = _make_db(tmp_path)
    request_id = db.create_request(
        raw_input="10.1000/race-two-workers", inferred_title="t",
        inferred_doi="10.1000/race-two-workers", inferred_url=None, paper_id=None,
    )
    # Both workers' view at read time: the freshly-created row's revision.
    revision_both_workers_read = db.get_request(request_id)["revision"]
    paper_id = db.upsert_located_paper(LocateCandidate(title="Race Two Workers Paper"))
    applied_a = db.update_request_status(
        request_id, "downloaded", paper_id=paper_id, expected_revision=revision_both_workers_read,
    )
    assert applied_a is True

    applied_b = db.update_request_status(
        request_id, "retrying",
        manual=ManualIntervention(reason="stale failure"),
        expected_revision=revision_both_workers_read,
    )
    assert applied_b is False

    row = db.get_request(request_id)
    assert row["status"] == "downloaded"
    assert row["paper_id"] == paper_id
    assert row["manual_reason"] is None


def test_update_request_status_compare_and_set_blocks_aba_stale_worker(tmp_path: Path):
    """MEDIUM review round-4 finding 1: status-alone CAS has an ABA race. Two
    force_redownload workers both read status='downloaded' (same revision N).
    The FASTER worker re-downloads and writes 'downloaded' AGAIN — same STATUS
    STRING, but a genuinely newer row (revision N+1). The SLOWER worker's
    stale failure, still holding revision N, must no-op even though `status`
    alone still reads "downloaded" and would satisfy a status-only CAS check —
    this is exactly the case status-based CAS (round 3) could not catch."""
    db = _make_db(tmp_path)
    request_id = db.create_request(
        raw_input="10.1000/aba-race", inferred_title="t",
        inferred_doi="10.1000/aba-race", inferred_url=None, paper_id=None,
    )
    paper_1 = db.upsert_located_paper(LocateCandidate(title="ABA Race Paper One"))
    db.update_request_status(request_id, "downloaded", paper_id=paper_1)
    revision_both_workers_read = db.get_request(request_id)["revision"]

    # The FASTER worker force-redownloads and succeeds again: status cycles
    # back to the SAME "downloaded" value, but the revision bumps.
    paper_2 = db.upsert_located_paper(LocateCandidate(title="ABA Race Paper Two"))
    applied_winner = db.update_request_status(
        request_id, "downloaded", paper_id=paper_2, expected_revision=revision_both_workers_read,
    )
    assert applied_winner is True
    row = db.get_request(request_id)
    assert row["paper_id"] == paper_2
    assert row["revision"] != revision_both_workers_read

    # The SLOWER worker's stale failure, still holding the OLD revision it read
    # before either worker did anything, must no-op.
    applied_stale = db.update_request_status(
        request_id, "retrying",
        manual=ManualIntervention(reason="stale failure"),
        expected_revision=revision_both_workers_read,
    )
    assert applied_stale is False

    row = db.get_request(request_id)
    assert row["status"] == "downloaded"
    assert row["paper_id"] == paper_2
    assert row["manual_reason"] is None


def test_update_request_status_non_success_still_applies_before_any_success(tmp_path: Path):
    """The guard must not block the NORMAL case: a fresh request's ordinary
    non-success transitions (in_progress -> failed, or a retry sequence) apply
    exactly as before, since the row was never in a success state."""
    db = _make_db(tmp_path)
    request_id = db.create_request(
        raw_input="10.1000/race-normal-failure", inferred_title="t",
        inferred_doi="10.1000/race-normal-failure", inferred_url=None, paper_id=None,
    )
    db.update_request_status(request_id, "in_progress")
    applied = db.update_request_status(request_id, "failed", manual=ManualIntervention(reason="bot wall"))
    assert applied is True
    row = db.get_request(request_id)
    assert row["status"] == "failed"
    assert row["manual_reason"] == "bot wall"
