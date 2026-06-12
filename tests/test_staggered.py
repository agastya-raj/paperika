from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from paperika.config import PaperikaConfig
from paperika.db import Database
from paperika.downloader import DownloadOutcome, Downloader
from paperika.staggered import run_staggered_random_worker
from paperika.worker import run_worker_once


class RecordingDownloader(Downloader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_entries = 0
        self.browser_token = object()
        self.calls: list[tuple[int, object | None]] = []

    @contextmanager
    def _connected_local_browser(self):
        self.browser_entries += 1
        yield self.browser_token

    def process_request(self, request_id: int, browser=None):
        self.calls.append((request_id, browser))
        self.db.update_request_status(request_id, "downloaded", attempt_count=1)
        request = self.db.get_request(request_id)
        return DownloadOutcome(
            request_id=request_id,
            paper_id=request["paper_id"] if request else None,
            status="downloaded",
            message="ok",
            local_pdf_path=f"/tmp/{request_id}.pdf",
            attempt_number=1,
        )


class ReverseShuffleRng:
    def shuffle(self, seq):
        seq.reverse()

    def uniform(self, a, b):
        return (a + b) / 2


def build_downloader(tmp_path: Path) -> RecordingDownloader:
    config = PaperikaConfig(
        db_path=tmp_path / "papers.db",
        download_dir=tmp_path / "downloads",
        screenshot_dir=tmp_path / "shots",
        notification_dir=tmp_path / "events",
    )
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    return RecordingDownloader(config, db)


def test_run_worker_once_can_limit_and_shuffle_due_requests(tmp_path: Path):
    downloader = build_downloader(tmp_path)
    first = downloader.enqueue("10.1000/a")
    second = downloader.enqueue("10.1000/b")
    third = downloader.enqueue("10.1000/c")

    result = run_worker_once(downloader, limit=2, shuffle=True, rng=ReverseShuffleRng(), browser=downloader.browser_token)

    assert result["processed_count"] == 2
    assert result["request_ids"] == [third["request_id"], second["request_id"]]
    assert downloader.calls == [
        (third["request_id"], downloader.browser_token),
        (second["request_id"], downloader.browser_token),
    ]
    remaining = [row["id"] for row in downloader.db.list_retryable_requests()]
    assert remaining == [first["request_id"]]


def test_run_staggered_random_worker_reuses_one_browser_session(tmp_path: Path):
    downloader = build_downloader(tmp_path)
    first = downloader.enqueue("10.1000/a")
    second = downloader.enqueue("10.1000/b")
    third = downloader.enqueue("10.1000/c")
    sleeps: list[float] = []

    result = run_staggered_random_worker(
        downloader,
        min_per_hour=3,
        max_per_hour=4,
        max_papers=3,
        rng=ReverseShuffleRng(),
        sleep_fn=sleeps.append,
    )

    assert result["processed_count"] == 3
    assert result["request_ids"] == [third["request_id"], second["request_id"], first["request_id"]]
    assert downloader.browser_entries == 1
    assert all(browser is downloader.browser_token for _, browser in downloader.calls)
    assert len(sleeps) == 2
    assert all(900 <= seconds <= 1200 for seconds in sleeps)


def test_run_staggered_random_worker_waits_for_next_due_retry(tmp_path: Path):
    downloader = build_downloader(tmp_path)
    queued = downloader.enqueue("10.1000/a")
    due_at = datetime.now(timezone.utc) + timedelta(seconds=30)
    downloader.db.update_request_status(
        queued["request_id"],
        "retrying",
        attempt_count=1,
        next_retry_at=due_at.isoformat(),
    )
    sleeps: list[float] = []

    def fake_sleep(seconds: float):
        sleeps.append(seconds)
        downloader.db.update_request_status(queued["request_id"], "queued", next_retry_at=None)

    result = run_staggered_random_worker(
        downloader,
        min_per_hour=3,
        max_per_hour=4,
        max_papers=1,
        rng=ReverseShuffleRng(),
        sleep_fn=fake_sleep,
    )

    assert result["processed_count"] == 1
    assert sleeps
    assert sleeps[0] >= 0
    assert downloader.browser_entries == 1
    assert downloader.calls == [(queued["request_id"], downloader.browser_token)]
