from pathlib import Path
import sqlite3

from paperika.config import PaperikaConfig
from paperika.db import Database


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
