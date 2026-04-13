from __future__ import annotations

from abc import ABC, abstractmethod
import json
from urllib.parse import quote_plus
from urllib.request import Request, urlopen

from .config import PaperikaConfig
from .models import LocateCandidate, LocateResult
from .normalize import normalize_url


class LocatorAdapter(ABC):
    name: str = "base"

    @abstractmethod
    def search(self, query: str, mode: str, shortlist_size: int) -> list[LocateCandidate]:
        raise NotImplementedError


class MockLocatorAdapter(LocatorAdapter):
    name = "mock"

    def search(self, query: str, mode: str, shortlist_size: int) -> list[LocateCandidate]:
        candidates = [
            LocateCandidate(
                title=f"{query} — Mock Paper {idx + 1}",
                authors=["Doe, Jane", "Roe, Richard"] if idx == 0 else ["Author, Example"],
                year=2024 - idx,
                doi=None,
                venue="MockConf",
                abstract=None,
                canonical_url=f"https://example.org/papers/{quote_plus(query)}/{idx + 1}",
                pdf_url=f"https://example.org/papers/{quote_plus(query)}/{idx + 1}.pdf",
                open_access_url=f"https://example.org/open/{quote_plus(query)}/{idx + 1}",
                source=self.name,
                confidence=max(0.1, 0.9 - idx * 0.1),
            )
            for idx in range(shortlist_size)
        ]
        return candidates


class CrossrefAdapter(LocatorAdapter):
    name = "crossref"

    def search(self, query: str, mode: str, shortlist_size: int) -> list[LocateCandidate]:
        url = f"https://api.crossref.org/works?query.bibliographic={quote_plus(query)}&rows={shortlist_size}"
        request = Request(url, headers={"User-Agent": "paperika/0.1.0 (mailto:none@example.invalid)"})
        with urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
        items = payload.get("message", {}).get("items", [])
        candidates: list[LocateCandidate] = []
        for item in items:
            title_list = item.get("title") or []
            link_values = item.get("link") or []
            resource = item.get("resource") or {}
            doi = item.get("DOI")
            canonical = resource.get("primary", {}).get("URL") or item.get("URL")
            pdf_link = next((link.get("URL") for link in link_values if (link.get("content-type") or "").lower() == "application/pdf"), None)
            authors = []
            for author in item.get("author", []) or []:
                name = " ".join(part for part in [author.get("given"), author.get("family")] if part)
                if name:
                    authors.append(name)
            year = None
            issued = item.get("issued", {}).get("date-parts", [])
            if issued and issued[0]:
                year = issued[0][0]
            candidates.append(
                LocateCandidate(
                    title=title_list[0] if title_list else None,
                    authors=authors,
                    year=year,
                    doi=doi.lower() if isinstance(doi, str) else None,
                    venue=(item.get("container-title") or [None])[0],
                    abstract=item.get("abstract"),
                    canonical_url=normalize_url(canonical) if canonical else None,
                    pdf_url=normalize_url(pdf_link) if pdf_link else None,
                    open_access_url=None,
                    alternate_urls=[normalize_url(item.get("URL"))] if item.get("URL") else [],
                    source=self.name,
                    confidence=None,
                    metadata={"type": item.get("type")},
                )
            )
        return candidates


class LocatorService:
    def __init__(self, config: PaperikaConfig, adapter: LocatorAdapter | None = None):
        self.config = config
        self.adapter = adapter or MockLocatorAdapter()

    def locate(self, query: str, mode: str = "auto", shortlist_size: int | None = None) -> LocateResult:
        shortlist = self._effective_shortlist_size(query=query, mode=mode, requested=shortlist_size)
        candidates = self.adapter.search(query=query, mode=mode, shortlist_size=shortlist)
        best = self._choose_best_first(candidates)
        return LocateResult(
            query=query,
            mode=mode,
            shortlist_size=shortlist,
            best_first_paper=best,
            candidates=candidates,
        )

    def _effective_shortlist_size(self, query: str, mode: str, requested: int | None) -> int:
        requested = requested or self.config.discovery_shortlist_size
        if mode == "discover":
            return requested
        if mode == "lookup":
            return 1
        lowered = query.lower().strip()
        if lowered.startswith("10.") or len(lowered.split()) >= 6:
            return 1
        return requested

    @staticmethod
    def _choose_best_first(candidates: list[LocateCandidate]) -> LocateCandidate | None:
        if not candidates:
            return None
        sorted_candidates = sorted(
            candidates,
            key=lambda candidate: (
                candidate.canonical_url is not None,
                candidate.open_access_url is not None,
                candidate.pdf_url is not None,
                candidate.confidence or 0,
                candidate.year or 0,
            ),
            reverse=True,
        )
        return sorted_candidates[0]


def create_locator(config: PaperikaConfig, provider: str) -> LocatorService:
    provider = provider.lower()
    if provider == "crossref":
        adapter: LocatorAdapter = CrossrefAdapter()
    elif provider == "mock":
        adapter = MockLocatorAdapter()
    else:
        raise ValueError(f"Unsupported provider: {provider}")
    return LocatorService(config=config, adapter=adapter)
