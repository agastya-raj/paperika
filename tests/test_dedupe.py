from pathlib import Path

from paperika.config import PaperikaConfig
from paperika.db import Database
from paperika.downloader import Downloader
from paperika.models import LocateCandidate


def test_enqueue_uses_verified_pdf_dedupe(tmp_path: Path):
    config = PaperikaConfig(
        db_path=tmp_path / "papers.db",
        download_dir=tmp_path / "downloads",
        screenshot_dir=tmp_path / "shots",
    )
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()

    paper_id = db.upsert_located_paper(
        LocateCandidate(title="Test Paper", doi="10.1000/test", canonical_url="https://example.org/paper", authors=[])
    )
    db.mark_paper_downloaded(paper_id, str(config.download_dir / "test-paper.pdf"))

    downloader = Downloader(config, db)
    result = downloader.enqueue("10.1000/test")
    assert result["status"] == "completed_deduped"
    assert result["paper_id"] == paper_id
