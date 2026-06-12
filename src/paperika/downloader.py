from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
import re
import shutil
import subprocess
import time
from typing import Any, Iterator
from urllib.parse import unquote, urlparse
from urllib.request import Request, urlopen

from .config import PaperikaConfig
from .db import Database, normalize_title
from .models import ManualIntervention, ParsedInput, LocateCandidate
from .normalize import infer_input, normalize_url, is_probable_pdf_url, is_probable_viewer_url, detect_publisher
from .notifications import NotificationEvent, build_notification_event, emit_notification_event
from .retry import next_retry_at, should_retry

try:
    from playwright.sync_api import sync_playwright
except Exception:  # pragma: no cover - optional at runtime
    sync_playwright = None


@dataclass(slots=True)
class DownloadOutcome:
    request_id: int
    paper_id: int | None
    status: str
    message: str
    local_pdf_path: str | None = None
    manual: ManualIntervention | None = None
    deduped: bool = False
    attempt_number: int = 0
    notification_events: list[NotificationEvent] = field(default_factory=list)


@dataclass(slots=True)
class PageMatchResult:
    score: int
    reuse_allowed: bool
    evidence: list[str] = field(default_factory=list)


@dataclass(slots=True)
class PdfIdentityCheck:
    ok: bool
    reason: str
    observed_title: str | None = None
    observed_doi: str | None = None


GENERIC_TITLE_WORDS = {
    "advanced",
    "analysis",
    "approach",
    "conference",
    "design",
    "for",
    "from",
    "ieee",
    "international",
    "journal",
    "letter",
    "letters",
    "methods",
    "model",
    "monitoring",
    "network",
    "networks",
    "novel",
    "of",
    "on",
    "optical",
    "paper",
    "proceedings",
    "research",
    "study",
    "system",
    "systems",
    "the",
    "using",
    "with",
}

DOI_PATTERN = re.compile(r"10\.\d{4,9}/[-._;()/:a-z0-9]+", re.IGNORECASE)


class Downloader:
    def __init__(self, config: PaperikaConfig, db: Database):
        self.config = config
        self.db = db

    def enqueue(self, raw_input: str, force_redownload: bool = False) -> dict[str, Any]:
        parsed = infer_input(raw_input)
        deduped = None if force_redownload else self.db.find_verified_pdf(doi=parsed.doi, title=parsed.title)
        if deduped:
            request_id = self.db.create_request(
                raw_input=raw_input,
                inferred_title=parsed.title,
                inferred_doi=parsed.doi,
                inferred_url=parsed.url,
                paper_id=deduped["id"],
                force_redownload=force_redownload,
            )
            self.db.update_request_status(request_id, status="completed_deduped", paper_id=deduped["id"])
            return {
                "request_id": request_id,
                "paper_id": deduped["id"],
                "status": "completed_deduped",
                "local_pdf_path": deduped["local_pdf_path"],
            }
        paper_id = self._ensure_paper_record(parsed)
        request_id = self.db.create_request(
            raw_input=raw_input,
            inferred_title=parsed.title,
            inferred_doi=parsed.doi,
            inferred_url=parsed.url,
            paper_id=paper_id,
            force_redownload=force_redownload,
        )
        return {"request_id": request_id, "paper_id": paper_id, "status": "queued"}

    def process_request(self, request_id: int, browser: Any | None = None) -> DownloadOutcome:
        request_row = self.db.get_request(request_id)
        if request_row is None:
            raise KeyError(f"Unknown request id {request_id}")
        parsed = infer_input(request_row["raw_input"])
        # Use stored inferred_url when raw_input lacked a URL (e.g. plain DOI)
        if not parsed.url and request_row["inferred_url"]:
            parsed = ParsedInput(
                raw_input=parsed.raw_input,
                title=parsed.title,
                doi=parsed.doi,
                url=request_row["inferred_url"],
                probable_pdf=is_probable_pdf_url(request_row["inferred_url"]),
                probable_viewer=is_probable_viewer_url(request_row["inferred_url"]),
                publisher_hint=detect_publisher(request_row["inferred_url"]),
            )
        previous_status = request_row["status"]
        paper_id = request_row["paper_id"]
        if previous_status in {"completed_deduped", "downloaded"} and not request_row["force_redownload"]:
            paper_row = self.db.find_verified_pdf(doi=parsed.doi, title=parsed.title)
            return DownloadOutcome(
                request_id=request_id,
                paper_id=paper_id,
                status=previous_status,
                message="Request already completed",
                local_pdf_path=paper_row["local_pdf_path"] if paper_row else None,
                deduped=previous_status == "completed_deduped",
                attempt_number=int(request_row["attempt_count"]),
            )
        if previous_status in {"manual_intervention", "permanent_failure"} and not request_row["force_redownload"]:
            return DownloadOutcome(
                request_id=request_id,
                paper_id=paper_id,
                status=previous_status,
                message="Request requires explicit retry or force-redownload to run again",
                attempt_number=int(request_row["attempt_count"]),
                manual=ManualIntervention(
                    reason=request_row["manual_reason"] or "Request is not currently retryable",
                    screenshot_path=request_row["manual_screenshot_path"],
                    page_title=request_row["manual_page_title"],
                    current_url=request_row["manual_current_url"],
                    suggested_next_action=request_row["manual_suggested_next_action"],
                ),
            )
        attempt_number = int(request_row["attempt_count"]) + 1

        if paper_id and not request_row["force_redownload"]:
            paper_row = self.db.find_verified_pdf(doi=parsed.doi, title=parsed.title)
            if paper_row:
                self.db.record_attempt(request_id, paper_row["id"], attempt_number, "deduped", "db_lookup", "Verified PDF already exists")
                self.db.update_request_status(request_id, "completed_deduped", attempt_count=attempt_number, paper_id=paper_row["id"])
                return DownloadOutcome(
                    request_id=request_id,
                    paper_id=paper_row["id"],
                    status="completed_deduped",
                    message="Verified PDF already existed",
                    local_pdf_path=paper_row["local_pdf_path"],
                    deduped=True,
                    attempt_number=attempt_number,
                )

        try:
            outcome = self._download_with_fallback(
                parsed,
                request_id=request_id,
                paper_id=paper_id,
                attempt_number=attempt_number,
                browser=browser,
            )
            if outcome.local_pdf_path and outcome.paper_id:
                self.db.mark_paper_downloaded(outcome.paper_id, outcome.local_pdf_path)
                self.db.update_request_status(request_id, "downloaded", attempt_count=attempt_number, paper_id=outcome.paper_id)
                self.db.record_attempt(request_id, outcome.paper_id, attempt_number, "downloaded", "fallback_ladder", outcome.message)
                outcome.notification_events.extend(
                    self._emit_notification_events(
                        request_id=request_id,
                        paper_id=outcome.paper_id,
                        previous_status=previous_status,
                        status="downloaded",
                        message=outcome.message,
                        attempt_number=attempt_number,
                    )
                )
            return outcome
        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            manual = self._capture_failure_artifact(request_id, attempt_number, message, parsed)
            self.db.record_attempt(
                request_id,
                paper_id,
                attempt_number,
                "failed",
                "fallback_ladder",
                message,
                screenshot_path=manual.screenshot_path,
                page_title=manual.page_title,
                current_url=manual.current_url,
            )
            if self._requires_manual_intervention(message):
                final_status = "manual_intervention"
                self.db.update_request_status(
                    request_id,
                    final_status,
                    attempt_count=attempt_number,
                    next_retry_at=None,
                    manual=manual,
                    paper_id=paper_id,
                )
                return DownloadOutcome(
                    request_id=request_id,
                    paper_id=paper_id,
                    status=final_status,
                    message=message,
                    manual=manual,
                    attempt_number=attempt_number,
                    notification_events=self._emit_notification_events(
                        request_id=request_id,
                        paper_id=paper_id,
                        previous_status=previous_status,
                        status=final_status,
                        message=message,
                        attempt_number=attempt_number,
                        manual=manual,
                    ),
                )
            if should_retry(attempt_number):
                final_status = "retrying"
                self.db.update_request_status(
                    request_id,
                    final_status,
                    attempt_count=attempt_number,
                    next_retry_at=next_retry_at(attempt_number).isoformat(),
                    manual=manual,
                    paper_id=paper_id,
                )
                return DownloadOutcome(
                    request_id=request_id,
                    paper_id=paper_id,
                    status=final_status,
                    message=message,
                    manual=manual,
                    attempt_number=attempt_number,
                    notification_events=self._emit_notification_events(
                        request_id=request_id,
                        paper_id=paper_id,
                        previous_status=previous_status,
                        status=final_status,
                        message=message,
                        attempt_number=attempt_number,
                        manual=manual,
                    ),
                )
            final_status = "permanent_failure"
            self.db.update_request_status(
                request_id,
                final_status,
                attempt_count=attempt_number,
                next_retry_at=None,
                manual=manual,
                paper_id=paper_id,
            )
            return DownloadOutcome(
                request_id=request_id,
                paper_id=paper_id,
                status=final_status,
                message=message,
                manual=manual,
                attempt_number=attempt_number,
                notification_events=self._emit_notification_events(
                    request_id=request_id,
                    paper_id=paper_id,
                    previous_status=previous_status,
                    status=final_status,
                    message=message,
                    attempt_number=attempt_number,
                    manual=manual,
                ),
            )

    def retry_pending(self, browser: Any | None = None) -> list[DownloadOutcome]:
        results = []
        for request in self.db.list_retryable_requests():
            results.append(self.process_request(int(request["id"]), browser=browser))
        return results

    def _ensure_paper_record(self, parsed: ParsedInput) -> int:
        candidate = LocateCandidate(
            title=parsed.title,
            doi=parsed.doi,
            canonical_url=parsed.url,
            pdf_url=parsed.url if parsed.probable_pdf else None,
            source="local_input",
            authors=[],
            alternate_urls=[],
            metadata={"publisher_hint": parsed.publisher_hint},
        )
        return self.db.upsert_located_paper(candidate)

    def _download_with_fallback(
        self,
        parsed: ParsedInput,
        request_id: int,
        paper_id: int | None,
        attempt_number: int,
        browser: Any | None = None,
    ) -> DownloadOutcome:
        if parsed.doi and not parsed.url:
            resolved = self._resolve_doi(parsed.doi)
            if resolved:
                parsed.url = resolved
                parsed.probable_pdf = parsed.probable_pdf or resolved.lower().endswith(".pdf")
                parsed.probable_viewer = parsed.probable_viewer or "pdf" in resolved.lower()

        if browser is not None:
            chrome_result = self._download_via_browser(
                browser,
                parsed,
                request_id=request_id,
                attempt_number=attempt_number,
            )
            if chrome_result:
                if paper_id is None:
                    paper_id = self._ensure_paper_record(parsed)
                return DownloadOutcome(request_id, paper_id, "downloaded", "Downloaded via local Chrome", chrome_result, attempt_number=attempt_number)

            if parsed.doi:
                resolved = self._resolve_doi(parsed.doi)
                if resolved and self._normalized_page_url(resolved) != self._normalized_page_url(parsed.url):
                    parsed.url = resolved
                    parsed.probable_pdf = parsed.probable_pdf or resolved.lower().endswith(".pdf")
                    parsed.probable_viewer = parsed.probable_viewer or "pdf" in resolved.lower()
                    chrome_result = self._download_via_browser(
                        browser,
                        parsed,
                        request_id=request_id,
                        attempt_number=attempt_number,
                    )
                    if chrome_result:
                        if paper_id is None:
                            paper_id = self._ensure_paper_record(parsed)
                        return DownloadOutcome(request_id, paper_id, "downloaded", "Resolved DOI and used local Chrome", chrome_result, attempt_number=attempt_number)
        else:
            with self._connected_local_browser() as connected_browser:
                chrome_result = self._download_via_browser(
                    connected_browser,
                    parsed,
                    request_id=request_id,
                    attempt_number=attempt_number,
                )
                if chrome_result:
                    if paper_id is None:
                        paper_id = self._ensure_paper_record(parsed)
                    return DownloadOutcome(request_id, paper_id, "downloaded", "Downloaded via local Chrome", chrome_result, attempt_number=attempt_number)

                if parsed.doi:
                    resolved = self._resolve_doi(parsed.doi)
                    if resolved and self._normalized_page_url(resolved) != self._normalized_page_url(parsed.url):
                        parsed.url = resolved
                        parsed.probable_pdf = parsed.probable_pdf or resolved.lower().endswith(".pdf")
                        parsed.probable_viewer = parsed.probable_viewer or "pdf" in resolved.lower()
                        chrome_result = self._download_via_browser(
                            connected_browser,
                            parsed,
                            request_id=request_id,
                            attempt_number=attempt_number,
                        )
                        if chrome_result:
                            if paper_id is None:
                                paper_id = self._ensure_paper_record(parsed)
                            return DownloadOutcome(request_id, paper_id, "downloaded", "Resolved DOI and used local Chrome", chrome_result, attempt_number=attempt_number)

        raise RuntimeError("Unable to find or open a matching local Chrome tab/viewer for this paper")

    def _resolve_doi(self, doi: str) -> str | None:
        url = f"https://doi.org/{doi}"
        request = Request(url, headers=self._browserish_headers())
        with urlopen(request, timeout=20) as response:
            return normalize_url(response.geturl())

    def _target_pdf_path(self, parsed: ParsedInput, paper_id: int | None, url: str | None = None) -> Path:
        safe_base = normalize_title(parsed.title) or normalize_title(parsed.doi) or f"paper {paper_id or 'unknown'}"
        slug = "_".join(safe_base.split())[:120] or f"paper_{paper_id or 'unknown'}"
        suffix = ".pdf"
        if url and url.lower().endswith(".pdf"):
            suffix = ".pdf"
        target = self.config.download_dir / f"{slug}{suffix}"
        target.parent.mkdir(parents=True, exist_ok=True)
        return target

    @contextmanager
    def _connected_local_browser(self) -> Iterator[Any | None]:
        if sync_playwright is None:
            yield None
            return
        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(self.config.chrome_cdp_url)
            yield browser

    def _download_via_local_chrome(self, parsed: ParsedInput, request_id: int, attempt_number: int) -> str | None:
        with self._connected_local_browser() as browser:
            return self._download_via_browser(browser, parsed, request_id=request_id, attempt_number=attempt_number)

    def _download_via_browser(self, browser: Any | None, parsed: ParsedInput, request_id: int, attempt_number: int) -> str | None:
        if browser is None:
            return None
        pages = self._collect_pages(browser)
        matched_page = self._find_matching_page(pages, parsed)
        if matched_page is None and parsed.url:
            context = browser.contexts[0] if browser.contexts else None
            if context is None:
                return None
            matched_page = context.new_page()
            matched_page.goto(parsed.url, wait_until="domcontentloaded", timeout=60000)
            matched_page.wait_for_timeout(2000)
        if matched_page is None:
            return None
        self._prepare_download_behavior(matched_page)
        screenshot_path = self.config.screenshot_dir / f"request_{request_id}_attempt_{attempt_number}_matched.png"
        try:
            matched_page.screenshot(path=str(screenshot_path), full_page=False)
        except Exception:
            pass
        direct_pdf = self._try_materialize_direct_pdf_page(matched_page, parsed)
        if direct_pdf is not None:
            return str(direct_pdf)
        existing_files = {path.resolve() for path in self.config.download_dir.glob("*")}
        download_clicked = self._click_pdf_download(matched_page)
        if not download_clicked and parsed.url and self._normalized_page_url(matched_page.url) != self._normalized_page_url(parsed.url):
            matched_page.goto(parsed.url, wait_until="domcontentloaded", timeout=60000)
            matched_page.wait_for_timeout(2000)
            self._prepare_download_behavior(matched_page)
            direct_pdf = self._try_materialize_direct_pdf_page(matched_page, parsed)
            if direct_pdf is not None:
                return str(direct_pdf)
            existing_files = {path.resolve() for path in self.config.download_dir.glob("*")}
            download_clicked = self._click_pdf_download(matched_page)
        if not download_clicked:
            pdf_candidate = self._find_pdf_link_candidate(matched_page)
            if pdf_candidate and self._normalized_page_url(pdf_candidate) != self._normalized_page_url(matched_page.url):
                matched_page.goto(pdf_candidate, wait_until="domcontentloaded", timeout=60000)
                direct_pdf, existing_files, download_clicked = self._stabilize_pdf_candidate_page(matched_page, parsed)
                if direct_pdf is not None:
                    return str(direct_pdf)
        if download_clicked:
            target = self._target_pdf_path(parsed, None, matched_page.url)
            materialized = self._materialize_clicked_download(target, existing_files)
            self._raise_if_download_identity_mismatch(parsed, materialized, matched_page)
            return str(materialized)
        return None

    def _collect_pages(self, browser: Any) -> list[Any]:
        pages = []
        for context in browser.contexts:
            pages.extend(context.pages)
        return pages

    def _find_matching_page(self, pages: list[Any], parsed: ParsedInput) -> Any | None:
        best_page = None
        best_result: PageMatchResult | None = None
        for page in pages:
            try:
                candidate_url = page.url or ""
                title = page.title()
            except Exception:
                continue
            result = self._evaluate_page_match(page_url=candidate_url, page_title=title, parsed=parsed)
            if not result.reuse_allowed:
                continue
            if best_result is None or result.score > best_result.score:
                best_result = result
                best_page = page
        return best_page

    def _click_pdf_download(self, page: Any) -> bool:
        selectors = [
            "[aria-label='Download']",
            "[title='Download']",
            "cr-icon-button[aria-label='Download']",
            "cr-icon-button#download",
            "viewer-download-controls button",
            "button[aria-label*='download' i]",
            "button[title*='download' i]",
            "#download",
            "#save",
        ]
        targets = [page, *page.frames]
        for target in targets:
            for selector in selectors:
                try:
                    locator = target.locator(selector).first
                    if locator.count() == 0:
                        continue
                    locator.click(timeout=1500)
                    return True
                except Exception:
                    continue
        return False

    def _find_pdf_link_candidate(self, page: Any) -> str | None:
        try:
            candidates = page.eval_on_selector_all(
                "a[href], button, [role='button']",
                """
                elements => elements.map(el => ({
                    href: el.href || el.getAttribute('href') || null,
                    text: (el.innerText || el.textContent || '').trim(),
                    aria: el.getAttribute('aria-label') || '',
                    title: el.getAttribute('title') || '',
                }))
                """,
            )
        except Exception:
            return None

        best_url = None
        best_score = 0
        for candidate in candidates:
            href = candidate.get("href")
            haystack = " ".join(
                str(candidate.get(key) or "") for key in ("text", "aria", "title", "href")
            ).lower()
            if not href:
                continue
            score = 0
            if "viewmedia" in href.lower() or "view_article" in href.lower():
                score += 5
            if "pdf" in haystack:
                score += 4
            if "get pdf" in haystack or "pdf article" in haystack or "download pdf" in haystack:
                score += 4
            if "full text" in haystack:
                score += 2
            if score > best_score:
                best_score = score
                best_url = href
        return best_url if best_score >= 4 else None

    def _browserish_headers(self) -> dict[str, str]:
        return {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
        }

    def _is_downloadable_pdf_url(self, url: str | None) -> bool:
        lowered = (url or "").lower()
        return lowered.endswith(".pdf") or "directpdfaccess" in lowered or "/pdf" in lowered

    def _stabilize_pdf_candidate_page(
        self,
        page: Any,
        parsed: ParsedInput,
        settle_attempts: int = 4,
        settle_delay_ms: int = 1000,
    ) -> tuple[Path | None, set[Path], bool]:
        existing_files: set[Path] = {path.resolve() for path in self.config.download_dir.glob("*")}
        for attempt in range(settle_attempts):
            direct_pdf = self._try_materialize_direct_pdf_page(page, parsed)
            if direct_pdf is not None:
                return direct_pdf, existing_files, False
            self._prepare_download_behavior(page)
            existing_files = {path.resolve() for path in self.config.download_dir.glob("*")}
            if self._click_pdf_download(page):
                return None, existing_files, True
            if attempt < settle_attempts - 1:
                page.wait_for_timeout(settle_delay_ms)
        direct_pdf = self._try_materialize_direct_pdf_page(page, parsed)
        if direct_pdf is not None:
            return direct_pdf, existing_files, False
        return None, existing_files, False

    def _materialize_direct_pdf_page(self, page: Any, parsed: ParsedInput) -> Path | None:
        if not self._is_downloadable_pdf_url(getattr(page, "url", None)):
            return None
        target = self._target_pdf_path(parsed, None, page.url)
        materialized = self._materialize_url_download(page.url, target)
        self._raise_if_download_identity_mismatch(parsed, materialized, page)
        return materialized

    def _try_materialize_direct_pdf_page(self, page: Any, parsed: ParsedInput) -> Path | None:
        try:
            return self._materialize_direct_pdf_page(page, parsed)
        except Exception:
            return None

    def _materialize_url_download(self, url: str, target: Path) -> Path:
        request = Request(url, headers=self._browserish_headers())
        with urlopen(request, timeout=30) as response:
            payload = response.read()
        if not payload.startswith(b"%PDF"):
            raise RuntimeError(f"Resolved PDF URL did not return PDF bytes: {url}")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
        return target

    def _page_matches(self, page_url: str, page_title: str | None, parsed: ParsedInput) -> bool:
        return self._evaluate_page_match(page_url=page_url, page_title=page_title, parsed=parsed).reuse_allowed

    def _page_match_score(self, page_url: str, page_title: str | None, parsed: ParsedInput) -> int:
        return self._evaluate_page_match(page_url=page_url, page_title=page_title, parsed=parsed).score

    def _evaluate_page_match(self, page_url: str, page_title: str | None, parsed: ParsedInput) -> PageMatchResult:
        normalized_title = normalize_title(page_title) or ""
        page_text = " ".join(part for part in [normalized_title, unquote((page_url or "").lower())] if part)
        evidence: list[str] = []
        score = 0

        doi_match = False
        if parsed.doi and self._text_contains_doi(page_text, parsed.doi):
            evidence.append("doi")
            score += 100
            doi_match = True

        url_match = False
        if parsed.url:
            url_match = self._urls_strongly_match(parsed.url, page_url)
            if url_match:
                evidence.append("url")
                score += 90

        title_strength = self._title_match_strength(parsed.title, page_title)
        if title_strength >= 3:
            evidence.append("title_strong")
            score += 35
        elif title_strength == 2:
            evidence.append("title_partial")
            score += 10

        if parsed.publisher_hint and parsed.publisher_hint in (page_url or "").lower():
            evidence.append("publisher")
            score += 2

        identifier_overlap = self._identifier_tokens(parsed.url) | self._identifier_tokens(parsed.doi)
        overlap_count = len(identifier_overlap.intersection(self._identifier_tokens(page_url)))
        if overlap_count:
            evidence.append(f"identifier_overlap:{overlap_count}")
            score += min(overlap_count, 3)

        reuse_allowed = False
        if parsed.doi:
            reuse_allowed = doi_match or url_match
        elif parsed.url:
            reuse_allowed = url_match
        else:
            reuse_allowed = title_strength >= 3

        return PageMatchResult(score=score, reuse_allowed=reuse_allowed, evidence=evidence)

    def _normalized_page_url(self, value: str | None) -> str:
        if not value:
            return ""
        try:
            return normalize_url(value)
        except Exception:
            return (value or "").strip().lower()

    def _identifier_tokens(self, value: str | None) -> set[str]:
        if not value:
            return set()
        lowered = unquote(value).lower()
        parsed = urlparse(lowered)
        candidates = [lowered, parsed.path, parsed.query, parsed.fragment]
        tokens: set[str] = set()
        for candidate in candidates:
            for chunk in candidate.replace("/", " ").replace("-", " ").replace("_", " ").replace(".", " ").split():
                cleaned = "".join(ch for ch in chunk if ch.isalnum())
                if len(cleaned) >= 5:
                    tokens.add(cleaned)
        return tokens

    def _normalize_doi_token(self, value: str | None) -> str | None:
        normalized = (value or "").strip().lower().rstrip(".,;)]")
        return normalized or None

    def _text_contains_doi(self, text: str, doi: str) -> bool:
        normalized_doi = self._normalize_doi_token(doi)
        if not normalized_doi or not text:
            return False
        for match in DOI_PATTERN.finditer(text):
            if self._normalize_doi_token(match.group(0)) == normalized_doi:
                return True
        return False

    def _urls_strongly_match(self, left: str | None, right: str | None) -> bool:
        normalized_left = self._normalized_page_url(left)
        normalized_right = self._normalized_page_url(right)
        if not normalized_left or not normalized_right:
            return False
        if normalized_left == normalized_right:
            return True
        parsed_left = urlparse(normalized_left)
        parsed_right = urlparse(normalized_right)
        if parsed_left.netloc != parsed_right.netloc:
            return False
        left_path = parsed_left.path.rstrip("/")
        right_path = parsed_right.path.rstrip("/")
        if not left_path or not right_path:
            return False
        if left_path == right_path:
            return True
        return False

    def _distinctive_title_tokens(self, title: str | None) -> list[str]:
        normalized = normalize_title(title) or ""
        return [token for token in normalized.split() if len(token) >= 5 and token not in GENERIC_TITLE_WORDS]

    def _text_contains_normalized_title(self, requested_title: str | None, observed_text: str | None) -> bool:
        normalized_requested = normalize_title(requested_title) or ""
        normalized_observed = normalize_title(observed_text) or ""
        if not normalized_requested or not normalized_observed:
            return False
        return normalized_requested in normalized_observed

    def _title_match_strength(self, requested_title: str | None, observed_title: str | None) -> int:
        requested_tokens = self._distinctive_title_tokens(requested_title)
        observed_normalized = normalize_title(observed_title) or ""
        if not requested_tokens or not observed_normalized:
            return 0
        matches = sum(1 for token in requested_tokens if token in observed_normalized)
        coverage = matches / len(requested_tokens)
        if len(requested_tokens) >= 3 and matches >= 3 and coverage >= 0.6:
            return 3
        if len(requested_tokens) >= 2 and matches >= 2 and coverage >= 0.5:
            return 2
        return 0

    def _extract_pdf_identity(self, pdf_path: Path) -> tuple[str | None, str | None]:
        observed_title: str | None = None
        observed_doi: str | None = None

        if shutil.which("pdfinfo"):
            try:
                result = subprocess.run(
                    ["pdfinfo", str(pdf_path)],
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=10,
                )
                if result.stdout:
                    for line in result.stdout.splitlines():
                        if line.lower().startswith("title:"):
                            candidate = line.split(":", 1)[1].strip()
                            if candidate:
                                observed_title = candidate
                                break
            except Exception:
                pass

        try:
            data = pdf_path.read_bytes()[:262144]
        except Exception:
            data = b""
        text = data.decode("latin-1", errors="ignore")
        if observed_title is None:
            title_match = re.search(r"/Title\s*\((.*?)\)", text, re.IGNORECASE | re.DOTALL)
            if title_match:
                candidate = " ".join(title_match.group(1).split())
                if candidate:
                    observed_title = candidate
        doi_match = DOI_PATTERN.search(text)
        if doi_match:
            observed_doi = doi_match.group(0).lower().rstrip(".,;)]")
        return observed_title, observed_doi

    def _extract_pdf_text(self, pdf_path: Path, max_chars: int = 20000) -> str:
        if shutil.which("pdftotext"):
            try:
                result = subprocess.run(
                    ["pdftotext", "-f", "1", "-l", "2", str(pdf_path), "-"],
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=10,
                )
                if result.stdout:
                    return result.stdout[:max_chars]
            except Exception:
                pass
        try:
            return pdf_path.read_bytes()[:max_chars].decode("latin-1", errors="ignore")
        except Exception:
            return ""

    def _verify_downloaded_pdf_identity(self, parsed: ParsedInput, pdf_path: Path) -> PdfIdentityCheck:
        observed_title, observed_doi = self._extract_pdf_identity(pdf_path)
        extracted_text = self._extract_pdf_text(pdf_path)
        normalized_observed_doi = self._normalize_doi_token(observed_doi)
        normalized_requested_doi = self._normalize_doi_token(parsed.doi)
        if normalized_requested_doi:
            if normalized_observed_doi == normalized_requested_doi:
                return PdfIdentityCheck(True, "Matched requested DOI in downloaded PDF", observed_title, normalized_observed_doi)
            # Optica/JOCN PDFs can expose a license DOI (e.g. 10.1364/oa_license_v2)
            # in low-level PDF bytes before the article DOI appears in rendered text.
            # Trust the rendered text if it contains the exact requested DOI.
            if normalized_requested_doi in extracted_text.lower():
                return PdfIdentityCheck(
                    True,
                    "Matched requested DOI in extracted PDF text despite non-article DOI in PDF metadata/bytes",
                    observed_title,
                    normalized_requested_doi,
                )
            if normalized_observed_doi:
                return PdfIdentityCheck(
                    False,
                    f"expected DOI {parsed.doi} but observed {normalized_observed_doi}",
                    observed_title,
                    normalized_observed_doi,
                )
            if parsed.title and self._text_contains_normalized_title(parsed.title, extracted_text):
                return PdfIdentityCheck(
                    True,
                    "Matched requested title in extracted PDF text after DOI was unavailable",
                    observed_title or parsed.title,
                    None,
                )
            return PdfIdentityCheck(
                False,
                "downloaded PDF did not expose the requested DOI or a strong title match in extracted text",
                observed_title,
                None,
            )

        title_strength = self._title_match_strength(parsed.title, observed_title)
        if parsed.title and title_strength >= 3:
            return PdfIdentityCheck(True, "Matched requested title in downloaded PDF", observed_title, normalized_observed_doi)
        if parsed.title and self._text_contains_normalized_title(parsed.title, extracted_text):
            return PdfIdentityCheck(True, "Matched requested title in extracted PDF text", observed_title or parsed.title, normalized_observed_doi)
        if observed_title:
            return PdfIdentityCheck(False, f"title mismatch for requested paper: observed '{observed_title}'", observed_title, normalized_observed_doi)
        return PdfIdentityCheck(False, "downloaded PDF did not expose a matching DOI or strong title", observed_title, normalized_observed_doi)

    def _raise_if_download_identity_mismatch(self, parsed: ParsedInput, pdf_path: Path, page: Any) -> None:
        identity = self._verify_downloaded_pdf_identity(parsed, pdf_path)
        if identity.ok:
            return
        raise RuntimeError(
            "Downloaded PDF identity mismatch: "
            f"target_doi={parsed.doi or 'n/a'}; "
            f"target_title={parsed.title or 'n/a'}; "
            f"observed_doi={identity.observed_doi or 'n/a'}; "
            f"observed_title={identity.observed_title or 'n/a'}; "
            f"page_url={getattr(page, 'url', None) or 'n/a'}; "
            f"page_title={page.title() or 'n/a'}; "
            f"pdf_path={pdf_path}; "
            f"reason={identity.reason}"
        )

    def _prepare_download_behavior(self, page: Any) -> None:
        try:
            session = page.context.new_cdp_session(page)
            params = {
                "behavior": "allow",
                "downloadPath": str(self.config.download_dir),
                "eventsEnabled": True,
            }
            try:
                session.send("Browser.setDownloadBehavior", params)
            except Exception:
                session.send("Page.setDownloadBehavior", params)
        except Exception:
            return

    def _materialize_clicked_download(self, target: Path, existing_files: set[Path], timeout_seconds: int = 30) -> Path:
        deadline = time.time() + timeout_seconds
        seen_files = set(existing_files)
        while time.time() < deadline:
            candidate = self._find_native_download(seen_files, suggested_name=None)
            if candidate is not None:
                target.parent.mkdir(parents=True, exist_ok=True)
                if candidate.resolve() != target.resolve():
                    shutil.copy2(candidate, target)
                if self._is_pdf_file(target):
                    return target
                seen_files.add(candidate.resolve())
            time.sleep(0.5)
        raise RuntimeError("Chrome reported a download click, but no non-empty PDF was materialized")

    def _materialize_download(self, download: Any, target: Path, existing_files: set[Path], timeout_seconds: int = 30) -> Path:
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            download.save_as(str(target))
        except Exception:
            pass
        if self._is_pdf_file(target):
            return target

        try:
            suggested_name = download.suggested_filename
        except Exception:
            suggested_name = None

        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            candidate = self._find_native_download(existing_files, suggested_name)
            if candidate is not None:
                if target.exists() and target.resolve() != candidate.resolve():
                    target.unlink()
                if candidate.resolve() != target.resolve():
                    shutil.copy2(candidate, target)
                return target
            time.sleep(0.5)

        if self._is_pdf_file(target):
            return target
        raise RuntimeError("Chrome reported a download, but no non-empty PDF was materialized")

    def _is_pdf_file(self, path: Path) -> bool:
        try:
            if not path.exists() or not path.is_file() or path.stat().st_size <= 0:
                return False
            with path.open("rb") as handle:
                return handle.read(4) == b"%PDF"
        except Exception:
            return False

    def _find_native_download(self, existing_files: set[Path], suggested_name: str | None) -> Path | None:
        if suggested_name:
            candidate = self.config.download_dir / suggested_name
            if self._is_pdf_file(candidate):
                return candidate
        for path in sorted(self.config.download_dir.glob("*"), key=lambda item: item.stat().st_mtime, reverse=True):
            resolved = path.resolve()
            if resolved in existing_files or path.suffix == ".crdownload" or not path.is_file():
                continue
            if self._is_pdf_file(path):
                return path
        return None

    def _requires_manual_intervention(self, message: str) -> bool:
        lowered = message.lower()
        triggers = [
            "captcha",
            "verify you are human",
            "sign in",
            "login",
            "consent",
            "access denied",
        ]
        return any(trigger in lowered for trigger in triggers)

    def _capture_failure_artifact(self, request_id: int, attempt_number: int, message: str, parsed: ParsedInput) -> ManualIntervention:
        suggested = "Open the paper in local Chrome, ensure the PDF/viewer tab is visible, then rerun retry-pending."
        if sync_playwright is None:
            path = self.config.screenshot_dir / f"request_{request_id}_attempt_{attempt_number}_failure.txt"
            path.write_text(message, encoding="utf-8")
            return ManualIntervention(reason=message, screenshot_path=str(path), suggested_next_action=suggested)
        try:
            with sync_playwright() as p:
                browser = p.chromium.connect_over_cdp(self.config.chrome_cdp_url)
                pages = self._collect_pages(browser)
                page = self._find_matching_page(pages, parsed) or (pages[-1] if pages else None)
                if page is None:
                    raise RuntimeError("No Chrome page available for failure capture")
                png_path = self.config.screenshot_dir / f"request_{request_id}_attempt_{attempt_number}_failure.png"
                page.screenshot(path=str(png_path), full_page=False)
                return ManualIntervention(
                    reason=message,
                    screenshot_path=str(png_path),
                    page_title=page.title(),
                    current_url=page.url,
                    suggested_next_action=suggested,
                )
        except Exception:
            path = self.config.screenshot_dir / f"request_{request_id}_attempt_{attempt_number}_failure.txt"
            path.write_text(message, encoding="utf-8")
            return ManualIntervention(reason=message, screenshot_path=str(path), suggested_next_action=suggested)

    def _emit_notification_events(
        self,
        *,
        request_id: int,
        paper_id: int | None,
        previous_status: str | None,
        status: str,
        message: str,
        attempt_number: int,
        manual: ManualIntervention | None = None,
    ) -> list[NotificationEvent]:
        event = build_notification_event(
            request_id=request_id,
            paper_id=paper_id,
            status_before=previous_status,
            status_after=status,
            message=message,
            attempt_number=attempt_number,
            manual=manual,
        )
        if event is None:
            return []
        emit_notification_event(self.config, event)
        return [event]


def outcome_to_dict(outcome: DownloadOutcome) -> dict[str, object]:
    return {
        "request_id": outcome.request_id,
        "paper_id": outcome.paper_id,
        "status": outcome.status,
        "message": outcome.message,
        "local_pdf_path": outcome.local_pdf_path,
        "deduped": outcome.deduped,
        "attempt_number": outcome.attempt_number,
        "manual": None if outcome.manual is None else {
            "reason": outcome.manual.reason,
            "screenshot_path": outcome.manual.screenshot_path,
            "page_title": outcome.manual.page_title,
            "current_url": outcome.manual.current_url,
            "suggested_next_action": outcome.manual.suggested_next_action,
        },
        "notification_events": [event.to_dict() for event in outcome.notification_events],
    }
