from pathlib import Path

from paperika.config import PaperikaConfig
from paperika.db import Database
from paperika.downloader import Downloader
from paperika.models import ManualIntervention
from paperika.worker import run_worker_once


class FailingDownloader(Downloader):
    def _download_via_local_chrome(self, parsed, request_id, attempt_number):
        return None

    def _resolve_doi(self, doi):
        return None

    def _capture_failure_artifact(self, request_id, attempt_number, message, parsed):
        artifact = self.config.screenshot_dir / f"request_{request_id}_attempt_{attempt_number}.png"
        artifact.write_bytes(b"png")
        return ManualIntervention(
            reason=message,
            screenshot_path=str(artifact),
            current_url="https://example.org/paper",
            page_title="Example Paper",
            suggested_next_action="Retry later",
        )


def test_run_worker_once_processes_due_requests_and_returns_events(tmp_path: Path):
    config = PaperikaConfig(
        db_path=tmp_path / "papers.db",
        download_dir=tmp_path / "downloads",
        screenshot_dir=tmp_path / "shots",
        notification_dir=tmp_path / "events",
    )
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    downloader = FailingDownloader(config, db)
    queued = downloader.enqueue("10.1000/test-worker")

    result = run_worker_once(downloader)
    assert result["processed_count"] == 1
    assert result["request_ids"] == [queued["request_id"]]
    assert result["outcomes"][0]["status"] == "retrying"
    assert result["notification_events"][0]["event_type"] == "first_failure"
