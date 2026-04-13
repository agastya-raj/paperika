from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path

from .config import PaperikaConfig
from .models import ManualIntervention


@dataclass(slots=True)
class NotificationEvent:
    event_type: str
    request_id: int
    paper_id: int | None
    status_before: str | None
    status_after: str
    message: str
    screenshot_path: str | None = None
    current_url: str | None = None
    page_title: str | None = None
    emitted_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def build_notification_event(
    *,
    request_id: int,
    paper_id: int | None,
    status_before: str | None,
    status_after: str,
    message: str,
    attempt_number: int,
    manual: ManualIntervention | None = None,
) -> NotificationEvent | None:
    event_type = notification_event_type(
        status_before=status_before,
        status_after=status_after,
        attempt_number=attempt_number,
    )
    if event_type is None:
        return None
    return NotificationEvent(
        event_type=event_type,
        request_id=request_id,
        paper_id=paper_id,
        status_before=status_before,
        status_after=status_after,
        message=message,
        screenshot_path=manual.screenshot_path if manual else None,
        current_url=manual.current_url if manual else None,
        page_title=manual.page_title if manual else None,
    )


def notification_event_type(*, status_before: str | None, status_after: str, attempt_number: int) -> str | None:
    if status_after == "retrying" and attempt_number == 1:
        return "first_failure"
    if status_after == "downloaded" and status_before in {"retrying", "manual_intervention", "permanent_failure"}:
        return "first_success_after_failure"
    if status_after == "manual_intervention":
        return "manual_intervention_needed"
    if status_after == "permanent_failure":
        return "final_failure"
    return None


def emit_notification_event(config: PaperikaConfig, event: NotificationEvent) -> Path:
    config.notification_dir.mkdir(parents=True, exist_ok=True)
    safe_timestamp = event.emitted_at.replace(":", "").replace("+", "_")
    path = config.notification_dir / f"{safe_timestamp}_request_{event.request_id}_{event.event_type}.json"
    path.write_text(json.dumps(event.to_dict(), indent=2), encoding="utf-8")
    return path
