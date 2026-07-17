from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Iterator

from .config import PaperikaConfig
from .models import LocateCandidate, ManualIntervention


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    attempt_count INTEGER NOT NULL DEFAULT 0,
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


@dataclass(slots=True)
class Database:
    path: Path

    @classmethod
    def from_config(cls, config: PaperikaConfig) -> "Database":
        return cls(path=config.db_path)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def init(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.executescript(SCHEMA_SQL)

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
    ) -> None:
        now = utc_now()
        with self.transaction() as conn:
            existing = conn.execute("SELECT * FROM paper_requests WHERE id = ?", (request_id,)).fetchone()
            if not existing:
                raise KeyError(f"Request {request_id} not found")
            conn.execute(
                """
                UPDATE paper_requests
                SET status = ?,
                    attempt_count = COALESCE(?, attempt_count),
                    next_retry_at = ?,
                    manual_reason = ?,
                    manual_screenshot_path = ?,
                    manual_page_title = ?,
                    manual_current_url = ?,
                    manual_suggested_next_action = ?,
                    paper_id = COALESCE(?, paper_id),
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    status,
                    attempt_count,
                    next_retry_at,
                    manual.reason if manual else None,
                    manual.screenshot_path if manual else None,
                    manual.page_title if manual else None,
                    manual.current_url if manual else None,
                    manual.suggested_next_action if manual else None,
                    paper_id,
                    now,
                    request_id,
                ),
            )

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
