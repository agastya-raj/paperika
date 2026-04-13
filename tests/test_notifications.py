import json
from pathlib import Path

from paperika.config import PaperikaConfig
from paperika.notifications import build_notification_event, emit_notification_event, notification_event_type


def test_notification_event_type_mapping():
    assert notification_event_type(status_before="queued", status_after="retrying", attempt_number=1) == "first_failure"
    assert notification_event_type(status_before="retrying", status_after="downloaded", attempt_number=2) == "first_success_after_failure"
    assert notification_event_type(status_before="retrying", status_after="manual_intervention", attempt_number=2) == "manual_intervention_needed"
    assert notification_event_type(status_before="retrying", status_after="permanent_failure", attempt_number=10) == "final_failure"
    assert notification_event_type(status_before="queued", status_after="downloaded", attempt_number=1) is None


def test_emit_notification_event_writes_json(tmp_path: Path):
    config = PaperikaConfig(notification_dir=tmp_path / "events")
    config.ensure_runtime_dirs()
    event = build_notification_event(
        request_id=7,
        paper_id=3,
        status_before="queued",
        status_after="retrying",
        message="first failure",
        attempt_number=1,
    )
    assert event is not None

    path = emit_notification_event(config, event)
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["event_type"] == "first_failure"
    assert payload["request_id"] == 7
