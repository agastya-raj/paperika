from datetime import datetime, timezone

from paperika.retry import compute_retry_delay_minutes, next_retry_at, should_retry


def test_backoff_schedule():
    assert compute_retry_delay_minutes(1) == 5
    assert compute_retry_delay_minutes(2) == 10
    assert compute_retry_delay_minutes(3) == 20
    assert compute_retry_delay_minutes(4) == 40


def test_should_retry_limits_attempts():
    assert should_retry(1) is True
    assert should_retry(9) is True
    assert should_retry(10) is False


def test_next_retry_at_uses_utc_now():
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert next_retry_at(2, now=now).isoformat() == datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc).isoformat()
