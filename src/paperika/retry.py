from __future__ import annotations

from datetime import datetime, timedelta, timezone

BASE_MINUTES = 5
MAX_TRIES = 10


def compute_retry_delay_minutes(attempt_number: int) -> int:
    if attempt_number < 1:
        raise ValueError("attempt_number must be >= 1")
    return BASE_MINUTES * (2 ** (attempt_number - 1))


def next_retry_at(attempt_number: int, now: datetime | None = None) -> datetime:
    now = now or datetime.now(timezone.utc)
    delay = compute_retry_delay_minutes(attempt_number)
    return now + timedelta(minutes=delay)


def should_retry(attempt_number: int) -> bool:
    return attempt_number < MAX_TRIES


