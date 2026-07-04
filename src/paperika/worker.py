from __future__ import annotations

import random
from typing import Any

from .downloader import Downloader, outcome_to_dict


def select_due_requests(
    downloader: Downloader,
    *,
    limit: int | None = None,
    shuffle: bool = False,
    rng: random.Random | None = None,
) -> list[Any]:
    due_requests = list(downloader.db.list_retryable_requests())
    if shuffle:
        (rng or random.Random()).shuffle(due_requests)
    if limit is not None:
        return due_requests[:limit]
    return due_requests


def run_worker_once(
    downloader: Downloader,
    *,
    limit: int | None = None,
    shuffle: bool = False,
    rng: random.Random | None = None,
    browser: Any | None = None,
) -> dict[str, object]:
    selected_requests = select_due_requests(downloader, limit=limit, shuffle=shuffle, rng=rng)
    outcomes = []
    notification_events = []
    for request in selected_requests:
        outcome = downloader.process_request(int(request["id"]), browser=browser)
        outcomes.append(outcome_to_dict(outcome))
        notification_events.extend(event.to_dict() for event in outcome.notification_events)
    return {
        "processed_count": len(outcomes),
        "request_ids": [outcome["request_id"] for outcome in outcomes],
        "outcomes": outcomes,
        "notification_events": notification_events,
    }
