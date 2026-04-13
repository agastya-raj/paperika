from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any


PaperMetadata = dict[str, Any]


@dataclass(slots=True)
class LinkCandidate:
    url: str
    link_type: str
    source: str | None = None
    score: float | None = None


@dataclass(slots=True)
class LocateCandidate:
    title: str | None = None
    authors: list[str] = field(default_factory=list)
    year: int | None = None
    doi: str | None = None
    venue: str | None = None
    abstract: str | None = None
    canonical_url: str | None = None
    pdf_url: str | None = None
    open_access_url: str | None = None
    alternate_urls: list[str] = field(default_factory=list)
    source: str | None = None
    confidence: float | None = None
    metadata: PaperMetadata = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class LocateResult:
    query: str
    mode: str
    shortlist_size: int
    best_first_paper: LocateCandidate | None
    candidates: list[LocateCandidate]
    generated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return {
            "query": self.query,
            "mode": self.mode,
            "shortlist_size": self.shortlist_size,
            "generated_at": self.generated_at,
            "best_first_paper": self.best_first_paper.to_dict() if self.best_first_paper else None,
            "candidates": [candidate.to_dict() for candidate in self.candidates],
        }


@dataclass(slots=True)
class ParsedInput:
    raw_input: str
    title: str | None = None
    doi: str | None = None
    url: str | None = None
    probable_pdf: bool = False
    probable_viewer: bool = False
    publisher_hint: str | None = None


@dataclass(slots=True)
class ManualIntervention:
    reason: str
    screenshot_path: str | None = None
    page_title: str | None = None
    current_url: str | None = None
    suggested_next_action: str | None = None
