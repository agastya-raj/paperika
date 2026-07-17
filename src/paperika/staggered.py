from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import random
import time
from typing import Callable

from .downloader import Downloader
from .worker import run_worker_once


@dataclass(slots=True)
class StaggeredRunConfig:
    min_per_hour: int = 3
    max_per_hour: int = 4
    max_papers: int | None = None
    seed: int | None = None

    def validate(self) -> None:
        if self.min_per_hour < 1 or self.max_per_hour < 1:
            raise ValueError("min_per_hour and max_per_hour must be >= 1")
        if self.min_per_hour > self.max_per_hour:
            raise ValueError("min_per_hour must be <= max_per_hour")
        if self.max_papers is not None and self.max_papers < 1:
            raise ValueError("max_papers must be >= 1 when provided")

    @property
    def min_interval_seconds(self) -> float:
        return 3600 / self.max_per_hour

    @property
    def max_interval_seconds(self) -> float:
        return 3600 / self.min_per_hour


def _seconds_until(iso_timestamp: str | None) -> float | None:
    if not iso_timestamp:
        return None
    target = datetime.fromisoformat(iso_timestamp)
    if target.tzinfo is None:
        target = target.replace(tzinfo=timezone.utc)
    delta = (target - datetime.now(timezone.utc)).total_seconds()
    return max(0.0, delta)


def run_staggered_random_worker(
    downloader: Downloader,
    *,
    min_per_hour: int = 3,
    max_per_hour: int = 4,
    max_papers: int | None = None,
    seed: int | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
) -> dict[str, object]:
    config = StaggeredRunConfig(
        min_per_hour=min_per_hour,
        max_per_hour=max_per_hour,
        max_papers=max_papers,
        seed=seed,
    )
    config.validate()
    rng = rng or random.Random(seed)

    outcomes: list[dict[str, object]] = []
    notification_events: list[dict[str, object]] = []
    sleep_intervals_seconds: list[float] = []

    with downloader._connected_local_browser() as browser:
        while True:
            result = run_worker_once(downloader, limit=1, shuffle=True, rng=rng, browser=browser)
            if result["processed_count"]:
                outcomes.extend(result["outcomes"])
                notification_events.extend(result["notification_events"])
                if config.max_papers is not None and len(outcomes) >= config.max_papers:
                    break
                if downloader.db.count_active_requests() == 0:
                    break
                sleep_seconds = rng.uniform(config.min_interval_seconds, config.max_interval_seconds)
                sleep_intervals_seconds.append(sleep_seconds)
                sleep_fn(sleep_seconds)
                continue

            active_requests = downloader.db.count_active_requests()
            if active_requests == 0:
                break
            next_due_seconds = _seconds_until(downloader.db.next_retryable_at())
            if next_due_seconds is None:
                break
            sleep_intervals_seconds.append(next_due_seconds)
            sleep_fn(next_due_seconds)

    return {
        "processed_count": len(outcomes),
        "request_ids": [outcome["request_id"] for outcome in outcomes],
        "outcomes": outcomes,
        "notification_events": notification_events,
        "sleep_intervals_seconds": sleep_intervals_seconds,
        "min_per_hour": min_per_hour,
        "max_per_hour": max_per_hour,
        "max_papers": max_papers,
        "seed": seed,
    }
