from __future__ import annotations

from .downloader import Downloader, outcome_to_dict


def run_worker_once(downloader: Downloader) -> dict[str, object]:
    due_requests = downloader.db.list_retryable_requests()
    outcomes = []
    notification_events = []
    for request in due_requests:
        outcome = downloader.process_request(int(request["id"]))
        outcomes.append(outcome_to_dict(outcome))
        notification_events.extend(event.to_dict() for event in outcome.notification_events)
    return {
        "processed_count": len(outcomes),
        "request_ids": [outcome["request_id"] for outcome in outcomes],
        "outcomes": outcomes,
        "notification_events": notification_events,
    }
