from pathlib import Path

from paperika.config import PaperikaConfig
from paperika.db import Database
from paperika.downloader import Downloader
from paperika.normalize import infer_input


class StubDownloader(Downloader):
    def __init__(self, *args, chrome_results=None, doi_result=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.chrome_results = list(chrome_results or [])
        self.doi_result = doi_result

    def _download_via_local_chrome(self, parsed, request_id, attempt_number):
        if not self.chrome_results:
            return None
        return self.chrome_results.pop(0)

    def _resolve_doi(self, doi):
        return self.doi_result


def test_download_fallback_prefers_local_chrome(tmp_path: Path):
    config = PaperikaConfig(
        db_path=tmp_path / "papers.db",
        download_dir=tmp_path / "downloads",
        screenshot_dir=tmp_path / "shots",
    )
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    downloader = StubDownloader(config, db, chrome_results=[str(config.download_dir / "x.pdf")])

    parsed = infer_input("https://example.org/paper.pdf")
    paper_id = downloader._ensure_paper_record(parsed)
    outcome = downloader._download_with_fallback(parsed, request_id=1, paper_id=paper_id, attempt_number=1)
    assert outcome.local_pdf_path.endswith("x.pdf")
    assert outcome.status == "downloaded"


def test_download_fallback_uses_doi_resolution(tmp_path: Path):
    config = PaperikaConfig(
        db_path=tmp_path / "papers.db",
        download_dir=tmp_path / "downloads",
        screenshot_dir=tmp_path / "shots",
    )
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    downloader = StubDownloader(
        config,
        db,
        chrome_results=[None, str(config.download_dir / "resolved.pdf")],
        doi_result="https://example.org/resolved.pdf",
    )

    parsed = infer_input("10.1000/test-doi")
    paper_id = downloader._ensure_paper_record(parsed)
    outcome = downloader._download_with_fallback(parsed, request_id=1, paper_id=paper_id, attempt_number=1)
    assert outcome.local_pdf_path.endswith("resolved.pdf")


def test_process_request_moves_to_retrying_on_failure(tmp_path: Path):
    config = PaperikaConfig(
        db_path=tmp_path / "papers.db",
        download_dir=tmp_path / "downloads",
        screenshot_dir=tmp_path / "shots",
    )
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()

    downloader = StubDownloader(config, db, chrome_results=[None], doi_result=None)
    queued = downloader.enqueue("Some missing paper 10.1000/test-fail")
    outcome = downloader.process_request(queued["request_id"])
    assert outcome.status == "retrying"
    assert outcome.manual is not None
    assert outcome.manual.screenshot_path is not None


def test_completed_request_is_not_reprocessed(tmp_path: Path):
    config = PaperikaConfig(
        db_path=tmp_path / "papers.db",
        download_dir=tmp_path / "downloads",
        screenshot_dir=tmp_path / "shots",
    )
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    pdf_path = config.download_dir / "existing.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n")

    downloader = StubDownloader(config, db, chrome_results=[])
    parsed = infer_input("Known paper")
    paper_id = downloader._ensure_paper_record(parsed)
    db.mark_paper_downloaded(paper_id, str(pdf_path))
    first = downloader.enqueue("Known paper")
    outcome = downloader.process_request(first["request_id"])
    assert outcome.status == "completed_deduped"
    assert outcome.attempt_number == 0


def test_target_pdf_path_sanitizes_doi(tmp_path: Path):
    config = PaperikaConfig(
        db_path=tmp_path / "papers.db",
        download_dir=tmp_path / "downloads",
        screenshot_dir=tmp_path / "shots",
    )
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    downloader = StubDownloader(config, db)

    parsed = infer_input("10.1000/test-doi")
    target = downloader._target_pdf_path(parsed, paper_id=None)
    assert target.parent == config.download_dir
    assert target.name == "10_1000_test_doi.pdf"
