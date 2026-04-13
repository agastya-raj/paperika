from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

from .config import PaperikaConfig
from .db import Database, normalize_title
from .models import ManualIntervention, ParsedInput, LocateCandidate
from .normalize import infer_input, normalize_url
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

    def process_request(self, request_id: int) -> DownloadOutcome:
        request_row = self.db.get_request(request_id)
        if request_row is None:
            raise KeyError(f"Unknown request id {request_id}")
        parsed = infer_input(request_row["raw_input"])
        status = request_row["status"]
        paper_id = request_row["paper_id"]
        if status in {"completed_deduped", "downloaded"} and not request_row["force_redownload"]:
            paper_row = self.db.find_verified_pdf(doi=parsed.doi, title=parsed.title)
            return DownloadOutcome(
                request_id=request_id,
                paper_id=paper_id,
                status=status,
                message="Request already completed",
                local_pdf_path=paper_row["local_pdf_path"] if paper_row else None,
                deduped=status == "completed_deduped",
                attempt_number=int(request_row["attempt_count"]),
            )
        if status in {"manual_intervention", "permanent_failure"} and not request_row["force_redownload"]:
            return DownloadOutcome(
                request_id=request_id,
                paper_id=paper_id,
                status=status,
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
            outcome = self._download_with_fallback(parsed, request_id=request_id, paper_id=paper_id, attempt_number=attempt_number)
            if outcome.local_pdf_path and outcome.paper_id:
                self.db.mark_paper_downloaded(outcome.paper_id, outcome.local_pdf_path)
                self.db.update_request_status(request_id, "downloaded", attempt_count=attempt_number, paper_id=outcome.paper_id)
                self.db.record_attempt(request_id, outcome.paper_id, attempt_number, "downloaded", "fallback_ladder", outcome.message)
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
                self.db.update_request_status(
                    request_id,
                    "manual_intervention",
                    attempt_count=attempt_number,
                    next_retry_at=None,
                    manual=manual,
                    paper_id=paper_id,
                )
                return DownloadOutcome(
                    request_id=request_id,
                    paper_id=paper_id,
                    status="manual_intervention",
                    message=message,
                    manual=manual,
                    attempt_number=attempt_number,
                )
            if should_retry(attempt_number):
                self.db.update_request_status(
                    request_id,
                    "retrying",
                    attempt_count=attempt_number,
                    next_retry_at=next_retry_at(attempt_number).isoformat(),
                    manual=manual,
                    paper_id=paper_id,
                )
                return DownloadOutcome(
                    request_id=request_id,
                    paper_id=paper_id,
                    status="retrying",
                    message=message,
                    manual=manual,
                    attempt_number=attempt_number,
                )
            self.db.update_request_status(
                request_id,
                "permanent_failure",
                attempt_count=attempt_number,
                next_retry_at=None,
                manual=manual,
                paper_id=paper_id,
            )
            return DownloadOutcome(
                request_id=request_id,
                paper_id=paper_id,
                status="permanent_failure",
                message=message,
                manual=manual,
                attempt_number=attempt_number,
            )

    def retry_pending(self) -> list[DownloadOutcome]:
        results = []
        for request in self.db.list_retryable_requests():
            results.append(self.process_request(int(request["id"])))
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

    def _download_with_fallback(self, parsed: ParsedInput, request_id: int, paper_id: int | None, attempt_number: int) -> DownloadOutcome:
        chrome_result = self._download_via_local_chrome(parsed, request_id=request_id, attempt_number=attempt_number)
        if chrome_result:
            if paper_id is None:
                paper_id = self._ensure_paper_record(parsed)
            return DownloadOutcome(request_id, paper_id, "downloaded", "Downloaded via local Chrome", chrome_result, attempt_number=attempt_number)

        if parsed.doi:
            resolved = self._resolve_doi(parsed.doi)
            if resolved:
                parsed.url = resolved
                parsed.probable_pdf = parsed.probable_pdf or resolved.lower().endswith(".pdf")
                parsed.probable_viewer = parsed.probable_viewer or "pdf" in resolved.lower()
                chrome_result = self._download_via_local_chrome(parsed, request_id=request_id, attempt_number=attempt_number)
                if chrome_result:
                    if paper_id is None:
                        paper_id = self._ensure_paper_record(parsed)
                    return DownloadOutcome(request_id, paper_id, "downloaded", "Resolved DOI and used local Chrome", chrome_result, attempt_number=attempt_number)

        raise RuntimeError("Unable to find or open a matching local Chrome tab/viewer for this paper")

    def _resolve_doi(self, doi: str) -> str | None:
        url = f"https://doi.org/{doi}"
        request = Request(url, headers={"Accept": "text/html,application/pdf;q=0.9,*/*;q=0.8", "User-Agent": "paperika/0.1.0"})
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

    def _download_via_local_chrome(self, parsed: ParsedInput, request_id: int, attempt_number: int) -> str | None:
        if sync_playwright is None:
            return None
        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(self.config.chrome_cdp_url)
            pages = self._collect_pages(browser)
            matched_page = self._find_matching_page(pages, parsed)
            if matched_page is None and parsed.url:
                context = browser.contexts[0] if browser.contexts else browser.new_context()
                matched_page = context.new_page()
                matched_page.goto(parsed.url, wait_until="domcontentloaded", timeout=60000)
                matched_page.wait_for_timeout(2000)
            if matched_page is None:
                return None
            screenshot_path = self.config.screenshot_dir / f"request_{request_id}_attempt_{attempt_number}_matched.png"
            matched_page.screenshot(path=str(screenshot_path), full_page=False)
            download = self._click_pdf_download(matched_page)
            if not download and parsed.url and matched_page.url != parsed.url:
                matched_page.goto(parsed.url, wait_until="domcontentloaded", timeout=60000)
                matched_page.wait_for_timeout(2000)
                download = self._click_pdf_download(matched_page)
            if download:
                target = self._target_pdf_path(parsed, None, matched_page.url)
                download.save_as(str(target))
                return str(target)
            return None

    def _collect_pages(self, browser: Any) -> list[Any]:
        pages = []
        for context in browser.contexts:
            pages.extend(context.pages)
        return pages

    def _find_matching_page(self, pages: list[Any], parsed: ParsedInput) -> Any | None:
        for page in pages:
            candidate_url = page.url or ""
            title = page.title()
            if self._page_matches(page_url=candidate_url, page_title=title, parsed=parsed):
                return page
        return None

    def _click_pdf_download(self, page: Any):
        selectors = [
            "[aria-label='Download']",
            "[title='Download']",
            "cr-icon-button[aria-label='Download']",
            "cr-icon-button#download",
            "viewer-download-controls button",
            "#download",
            "#save",
        ]
        targets = [page, *page.frames]
        for target in targets:
            for selector in selectors:
                try:
                    with page.expect_download(timeout=3000) as download_info:
                        target.locator(selector).first.click(timeout=1500)
                    return download_info.value
                except Exception:
                    continue
        return None

    def _page_matches(self, page_url: str, page_title: str | None, parsed: ParsedInput) -> bool:
        title = (page_title or "").lower()
        url = (page_url or "").lower()
        checks = []
        if parsed.doi:
            checks.append(parsed.doi.lower() in url or parsed.doi.lower() in title)
        if parsed.url:
            checks.append(parsed.url.lower() == url or parsed.url.lower() in url)
        if parsed.title:
            title_words = [word for word in normalize_title(parsed.title).split() if len(word) > 3]
            checks.append(sum(word in title or word in url for word in title_words[:6]) >= min(2, len(title_words[:6]) or 0))
        return any(checks)

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
