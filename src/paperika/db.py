from __future__ import annotations

from contextlib import contextmanager
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Iterator

from .config import PaperikaConfig
from .models import LocateCandidate, ManualIntervention


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


# paper_requests.status values meaning "a paper was actually obtained" — used by
# update_request_status to decide whether a stale manual_* operator handoff must
# be cleared (AGA-596 P5 follow-up), and to guard against a stale non-success
# transition overwriting a request that already succeeded (AGA-596 review
# finding 3). The two callers spell success differently: downloader.py's
# queue-retry path uses "downloaded"/"completed_deduped"; the bridge uses
# "completed".
_SUCCESS_STATUSES = frozenset({"downloaded", "completed_deduped", "completed"})
# Same set, pre-rendered as a SQL literal list for the WHERE-clause guard below —
# built from _SUCCESS_STATUSES (not duplicated by hand) so the two can't drift.
_SUCCESS_STATUSES_SQL = ", ".join(f"'{s}'" for s in sorted(_SUCCESS_STATUSES))


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS papers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    normalized_title TEXT,
    doi TEXT UNIQUE,
    status TEXT NOT NULL DEFAULT 'new',
    local_pdf_path TEXT,
    verified_pdf INTEGER NOT NULL DEFAULT 0,
    canonical_url TEXT,
    open_access_url TEXT,
    year INTEGER,
    venue TEXT,
    authors_json TEXT,
    abstract TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    paper_id INTEGER NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    link_type TEXT NOT NULL,
    source TEXT,
    is_canonical INTEGER NOT NULL DEFAULT 0,
    confidence REAL,
    created_at TEXT NOT NULL,
    UNIQUE(paper_id, url)
);

CREATE TABLE IF NOT EXISTS paper_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_input TEXT NOT NULL,
    paper_id INTEGER REFERENCES papers(id) ON DELETE SET NULL,
    inferred_title TEXT,
    inferred_doi TEXT,
    inferred_url TEXT,
    source_mode TEXT NOT NULL DEFAULT 'local_download',
    status TEXT NOT NULL DEFAULT 'queued',
    priority INTEGER NOT NULL DEFAULT 100,
    force_redownload INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT,
    -- attempt_count counts WHOLE-REQUEST retries (downloader.py's queue-retry
    -- path bumps it per pass through process_request). It is a distinct counter
    -- from paper_attempts.attempt_number, which counts per-request TIER attempts
    -- (direct_fetch/scripted_browser/codex_bridge) inside the bridge's ladder.
    -- The bridge NEVER writes to this column — an earlier revision briefly did
    -- (AGA-596 P4), which would have corrupted downloader.py's own retry
    -- backoff/budget semantics for that counter; reverted (AGA-596 review round
    -- 2, finding 2). The tier-attempt count is instead a DERIVED field (a COUNT
    -- of paper_attempts rows) surfaced only in the bridge's GET /requests/{id}
    -- response.
    attempt_count INTEGER NOT NULL DEFAULT 0,
    -- revision is a pure optimistic-concurrency-control token (AGA-596 review
    -- round 4, finding 1) — NOT a semantic/operator-facing counter like
    -- attempt_count. update_request_status bumps it by 1 on every write it
    -- actually applies (never on a guarded no-op). A CAS caller's
    -- expected_revision must equal the row's CURRENT revision, not just its
    -- status string: status-only CAS has an ABA hole — e.g. two
    -- force_redownload workers both read status='downloaded'; the faster one
    -- re-downloads and writes 'downloaded' again (same STRING, but a genuinely
    -- newer row); the slower one's stale expected_status='downloaded' would
    -- still match and clobber that newer success. revision always changes on
    -- a real write, so it catches this where status alone cannot.
    revision INTEGER NOT NULL DEFAULT 0,
    manual_reason TEXT,
    manual_screenshot_path TEXT,
    manual_page_title TEXT,
    manual_current_url TEXT,
    manual_suggested_next_action TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id INTEGER NOT NULL REFERENCES paper_requests(id) ON DELETE CASCADE,
    paper_id INTEGER REFERENCES papers(id) ON DELETE SET NULL,
    attempt_number INTEGER NOT NULL,
    status TEXT NOT NULL,
    strategy TEXT,
    message TEXT,
    screenshot_path TEXT,
    page_title TEXT,
    current_url TEXT,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_papers_normalized_title ON papers(normalized_title);
CREATE INDEX IF NOT EXISTS idx_paper_requests_status_next_retry ON paper_requests(status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_paper_attempts_request_id ON paper_attempts(request_id);
"""


def _ensure_request_revision_column(conn: sqlite3.Connection) -> None:
    """Idempotent ``ALTER TABLE ... ADD COLUMN`` for ``paper_requests.revision``
    (AGA-596 review round 4, finding 1). ``CREATE TABLE IF NOT EXISTS`` in
    SCHEMA_SQL only creates the column on a brand-new database — it silently
    no-ops on a database whose ``paper_requests`` table already existed before
    this column was added (in particular the deployed gpu-box DB), so a
    pre-existing table needs this retrofit. Mirrors the same idiom the bridge
    already uses for its own ``paper_attempts`` columns (see
    ``bridge/limits.py:ensure_bridge_columns``), but lives here — not
    bridge-only — because ``paper_requests`` and its CAS use of ``revision``
    are shared by both downloader.py and the bridge, and this runs from
    ``Database.init()``, the one common entry point both go through."""
    existing = {row[1] for row in conn.execute("PRAGMA table_info(paper_requests)")}
    if "revision" not in existing:
        conn.execute("ALTER TABLE paper_requests ADD COLUMN revision INTEGER NOT NULL DEFAULT 0")
        conn.commit()


@dataclass(slots=True)
class Database:
    path: Path
    # Extra idempotent schema steps (e.g. the bridge's paper_attempts columns)
    # re-applied whenever connect() rebuilds a missing/empty DB file.
    schema_hooks: list[Callable[[sqlite3.Connection], None]] = field(default_factory=list)

    @classmethod
    def from_config(cls, config: PaperikaConfig) -> "Database":
        return cls(path=config.db_path)

    def connect(self) -> sqlite3.Connection:
        # A long-running bridge only ran init() at startup; when the DB file was
        # deleted underneath it (AGA-1250) every later connect() opened a fresh,
        # table-less file and failed with "no such table". Re-apply the schema
        # whenever the file is missing or empty (a stat per connect, idempotent).
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fresh = not self.path.exists() or self.path.stat().st_size == 0
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        if fresh:
            conn.executescript(SCHEMA_SQL)
            _ensure_request_revision_column(conn)
            for hook in self.schema_hooks:
                hook(conn)
        return conn

    def init(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.executescript(SCHEMA_SQL)
            _ensure_request_revision_column(conn)

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self.connect() as conn:
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def upsert_located_paper(self, candidate: LocateCandidate) -> int:
        normalized_title = normalize_title(candidate.title)
        with self.transaction() as conn:
            row = None
            if candidate.doi:
                row = conn.execute("SELECT id FROM papers WHERE doi = ?", (candidate.doi,)).fetchone()
            if row is None and normalized_title:
                row = conn.execute(
                    "SELECT id FROM papers WHERE normalized_title = ? ORDER BY verified_pdf DESC, id ASC LIMIT 1",
                    (normalized_title,),
                ).fetchone()
            now = utc_now()
            authors_json = json.dumps(candidate.authors)
            if row:
                paper_id = row[0]
                conn.execute(
                    """
                    UPDATE papers
                    SET title = COALESCE(?, title),
                        normalized_title = COALESCE(?, normalized_title),
                        doi = COALESCE(?, doi),
                        canonical_url = COALESCE(?, canonical_url),
                        open_access_url = COALESCE(?, open_access_url),
                        year = COALESCE(?, year),
                        venue = COALESCE(?, venue),
                        authors_json = CASE WHEN ? != '[]' THEN ? ELSE authors_json END,
                        abstract = COALESCE(?, abstract),
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        candidate.title,
                        normalized_title,
                        candidate.doi,
                        candidate.canonical_url,
                        candidate.open_access_url,
                        candidate.year,
                        candidate.venue,
                        authors_json,
                        authors_json,
                        candidate.abstract,
                        now,
                        paper_id,
                    ),
                )
            else:
                cursor = conn.execute(
                    """
                    INSERT INTO papers (
                        title, normalized_title, doi, canonical_url, open_access_url,
                        year, venue, authors_json, abstract, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        candidate.title,
                        normalized_title,
                        candidate.doi,
                        candidate.canonical_url,
                        candidate.open_access_url,
                        candidate.year,
                        candidate.venue,
                        authors_json,
                        candidate.abstract,
                        now,
                        now,
                    ),
                )
                paper_id = int(cursor.lastrowid)

            links: list[tuple[str, str, str | None, int, float | None]] = []
            for url in [candidate.canonical_url, candidate.pdf_url, candidate.open_access_url, *candidate.alternate_urls]:
                if not url:
                    continue
                link_type = "alternate"
                if url == candidate.canonical_url:
                    link_type = "canonical"
                elif url == candidate.pdf_url:
                    link_type = "pdf"
                elif url == candidate.open_access_url:
                    link_type = "open_access"
                links.append((url, link_type, candidate.source, 1 if link_type == "canonical" else 0, candidate.confidence))
            for url, link_type, source, is_canonical, confidence in links:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO paper_links
                    (paper_id, url, link_type, source, is_canonical, confidence, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (paper_id, url, link_type, source, is_canonical, confidence, now),
                )
            return paper_id

    def create_request(
        self,
        raw_input: str,
        inferred_title: str | None,
        inferred_doi: str | None,
        inferred_url: str | None,
        paper_id: int | None,
        force_redownload: bool = False,
    ) -> int:
        now = utc_now()
        with self.transaction() as conn:
            cursor = conn.execute(
                """
                INSERT INTO paper_requests (
                    raw_input, paper_id, inferred_title, inferred_doi, inferred_url,
                    status, force_redownload, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'queued', ?, ?, ?)
                """,
                (raw_input, paper_id, inferred_title, inferred_doi, inferred_url, 1 if force_redownload else 0, now, now),
            )
            return int(cursor.lastrowid)

    def find_verified_pdf(self, doi: str | None = None, title: str | None = None) -> sqlite3.Row | None:
        with self.connect() as conn:
            if doi:
                row = conn.execute(
                    "SELECT * FROM papers WHERE doi = ? AND verified_pdf = 1 AND local_pdf_path IS NOT NULL",
                    (doi,),
                ).fetchone()
                if row:
                    return row
            normalized_title = normalize_title(title)
            if normalized_title:
                return conn.execute(
                    """
                    SELECT * FROM papers
                    WHERE normalized_title = ? AND verified_pdf = 1 AND local_pdf_path IS NOT NULL
                    ORDER BY id ASC LIMIT 1
                    """,
                    (normalized_title,),
                ).fetchone()
            return None

    def get_request(self, request_id: int) -> sqlite3.Row | None:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM paper_requests WHERE id = ?", (request_id,)).fetchone()

    def list_retryable_requests(self, now_iso: str | None = None) -> list[sqlite3.Row]:
        now_iso = now_iso or utc_now()
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT * FROM paper_requests
                WHERE status IN ('queued', 'retrying')
                  AND (next_retry_at IS NULL OR next_retry_at <= ?)
                ORDER BY priority ASC, created_at ASC
                """,
                (now_iso,),
            ).fetchall()

    def count_active_requests(self) -> int:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM paper_requests WHERE status IN ('queued', 'retrying')"
            ).fetchone()
            return int(row[0]) if row else 0

    def next_retryable_at(self) -> str | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT MIN(next_retry_at)
                FROM paper_requests
                WHERE status IN ('queued', 'retrying')
                  AND next_retry_at IS NOT NULL
                """
            ).fetchone()
            return row[0] if row and row[0] else None

    def update_request_status(
        self,
        request_id: int,
        status: str,
        attempt_count: int | None = None,
        next_retry_at: str | None = None,
        manual: ManualIntervention | None = None,
        paper_id: int | None = None,
        expected_revision: int | None = None,
    ) -> bool:
        """Returns True if the row was actually updated, False if a guard below
        turned this into a no-op (AGA-596 review round 2/3/4) — callers that
        emit a status-change notification/event MUST check this before doing
        so, or they can report a status the row never actually reached. Raises
        KeyError only when ``request_id`` doesn't exist at all (a guarded
        no-op is NOT an error — the row exists, the write was just skipped).

        Two independent guard mechanisms, never both active at once:
        - Default (``expected_revision=None``): the success-status guard — a
          transition to a NON-success status cannot overwrite a row whose
          CURRENT status is already in ``_SUCCESS_STATUSES`` (protects against
          an unintentional stale write resurrecting an already-succeeded
          request). This is the only guard for callers that never supply
          ``expected_revision`` — in particular every bridge call site.
        - ``expected_revision`` (compare-and-set on ``paper_requests.revision``,
          AGA-596 review round 4 finding 1): downloader.py's
          ``process_request`` reads the row once at the top and does real work
          (network I/O) before writing back. Round 3 CAS'd on ``expected_status``
          (the STATUS STRING read at that same moment) instead — replacing an
          even earlier, unconditional ``allow_reopen=True`` that let ANY
          concurrent write bypass the success guard, race or not — but
          status-only CAS has an ABA hole: two force_redownload workers can
          both read status='downloaded'; the faster one re-downloads and
          writes 'downloaded' AGAIN (same STRING, genuinely newer row); the
          slower one's stale expected_status='downloaded' would still match
          and clobber that newer success. ``revision`` is bumped by 1 on every
          write this method actually applies (never on a guarded no-op, under
          either mechanism), so it always differs after a real write even when
          the status string cycles back to what it was — CAS on it is
          therefore strictly stronger than status-only CAS and fully replaces
          the success guard for a CAS call rather than composing with it.
        """
        now = utc_now()
        # A request reaching a SUCCESS status must never keep showing a stale
        # operator handoff from an earlier failed attempt (AGA-596 P5 follow-up):
        # clear manual_* unconditionally on success, regardless of whether `manual`
        # was passed. Every other status keeps the only-overwrite-when-supplied
        # (COALESCE) behavior, so a later transition without a fresh
        # ManualIntervention cannot erase a still-relevant handoff. This applies
        # under EITHER guard mechanism below.
        is_success = 1 if status in _SUCCESS_STATUSES else 0
        manual_params = (
            is_success, manual.reason if manual else None,
            is_success, manual.screenshot_path if manual else None,
            is_success, manual.page_title if manual else None,
            is_success, manual.current_url if manual else None,
            is_success, manual.suggested_next_action if manual else None,
        )
        # revision = revision + 1 fires only when the WHERE clause matches (i.e.
        # only on an ACTUALLY applied write) — a guarded no-op never bumps it.
        set_clause = """
            SET status = ?,
                attempt_count = COALESCE(?, attempt_count),
                revision = revision + 1,
                next_retry_at = ?,
                manual_reason = CASE WHEN ? THEN NULL ELSE COALESCE(?, manual_reason) END,
                manual_screenshot_path = CASE WHEN ? THEN NULL ELSE COALESCE(?, manual_screenshot_path) END,
                manual_page_title = CASE WHEN ? THEN NULL ELSE COALESCE(?, manual_page_title) END,
                manual_current_url = CASE WHEN ? THEN NULL ELSE COALESCE(?, manual_current_url) END,
                manual_suggested_next_action = CASE WHEN ? THEN NULL ELSE COALESCE(?, manual_suggested_next_action) END,
                paper_id = COALESCE(?, paper_id),
                updated_at = ?
        """
        with self.transaction() as conn:
            existing = conn.execute("SELECT * FROM paper_requests WHERE id = ?", (request_id,)).fetchone()
            if not existing:
                raise KeyError(f"Request {request_id} not found")
            if expected_revision is not None:
                cursor = conn.execute(
                    f"UPDATE paper_requests {set_clause} WHERE id = ? AND revision = ?",
                    (status, attempt_count, next_retry_at, *manual_params, paper_id, now,
                     request_id, expected_revision),
                )
            else:
                # `status` inside the WHERE clause refers to the row's CURRENT
                # (pre-update) value, per standard UPDATE semantics — this is a
                # single guarded statement, not a separate read-then-write, so
                # there is no check-then-act gap. A transition that is ITSELF a
                # success status is never blocked.
                cursor = conn.execute(
                    f"""
                    UPDATE paper_requests {set_clause}
                    WHERE id = ? AND NOT (status IN ({_SUCCESS_STATUSES_SQL}) AND ? = 0)
                    """,
                    (status, attempt_count, next_retry_at, *manual_params, paper_id, now,
                     request_id, is_success),
                )
            return cursor.rowcount > 0

    def record_attempt(
        self,
        request_id: int,
        paper_id: int | None,
        attempt_number: int,
        status: str,
        strategy: str,
        message: str | None = None,
        screenshot_path: str | None = None,
        page_title: str | None = None,
        current_url: str | None = None,
    ) -> int:
        with self.transaction() as conn:
            cursor = conn.execute(
                """
                INSERT INTO paper_attempts (
                    request_id, paper_id, attempt_number, status, strategy,
                    message, screenshot_path, page_title, current_url, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (request_id, paper_id, attempt_number, status, strategy, message, screenshot_path, page_title, current_url, utc_now()),
            )
            return int(cursor.lastrowid)

    def mark_paper_downloaded(self, paper_id: int, local_pdf_path: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                """
                UPDATE papers
                SET local_pdf_path = ?, verified_pdf = 1, status = 'downloaded', updated_at = ?
                WHERE id = ?
                """,
                (local_pdf_path, utc_now(), paper_id),
            )


def normalize_title(title: str | None) -> str | None:
    if not title:
        return None
    text = title.lower().strip()
    text = " ".join(text.split())
    text = "".join(ch if ch.isalnum() or ch.isspace() else " " for ch in text)
    text = " ".join(text.split())
    return text or None
