from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import shutil
import time
from typing import Any
from urllib.parse import unquote, urlparse
from urllib.request import Request, urlopen

from .config import PaperikaConfig
from .db import Database, normalize_title
from .models import ManualIntervention, ParsedInput, LocateCandidate
from .normalize import infer_input, normalize_url
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
            outcome = self._download_with_fallback(parsed, request_id=request_id, paper_id=paper_id, attempt_number=attempt_number)
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
            matched_page.screenshot(path=str(screenshot_path), full_page=False)
            existing_files = {path.resolve() for path in self.config.download_dir.glob("*")}
            download = self._click_pdf_download(matched_page)
            if not download and parsed.url and self._normalized_page_url(matched_page.url) != self._normalized_page_url(parsed.url):
                matched_page.goto(parsed.url, wait_until="domcontentloaded", timeout=60000)
                matched_page.wait_for_timeout(2000)
                self._prepare_download_behavior(matched_page)
                existing_files = {path.resolve() for path in self.config.download_dir.glob("*")}
                download = self._click_pdf_download(matched_page)
            if download:
                target = self._target_pdf_path(parsed, None, matched_page.url)
                return str(self._materialize_download(download, target, existing_files))
            return None

    def _collect_pages(self, browser: Any) -> list[Any]:
        pages = []
        for context in browser.contexts:
            pages.extend(context.pages)
        return pages

    def _find_matching_page(self, pages: list[Any], parsed: ParsedInput) -> Any | None:
        best_page = None
        best_score = 0
        for page in pages:
            try:
                candidate_url = page.url or ""
                title = page.title()
            except Exception:
                continue
            score = self._page_match_score(page_url=candidate_url, page_title=title, parsed=parsed)
            if score > best_score:
                best_score = score
                best_page = page
        return best_page if best_score >= 2 else None

    def _click_pdf_download(self, page: Any):
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
                    with page.expect_download(timeout=3000) as download_info:
                        target.locator(selector).first.click(timeout=1500)
                    return download_info.value
                except Exception:
                    continue
        return None

    def _page_matches(self, page_url: str, page_title: str | None, parsed: ParsedInput) -> bool:
        return self._page_match_score(page_url=page_url, page_title=page_title, parsed=parsed) >= 2

    def _page_match_score(self, page_url: str, page_title: str | None, parsed: ParsedInput) -> int:
        title = normalize_title(page_title) or ""
        url = (page_url or "").lower()
        url_tokens = self._identifier_tokens(url)
        score = 0
        if parsed.doi:
            doi = parsed.doi.lower()
            if doi in url or doi in title:
                score += 4
            elif doi.replace("/", "") in url.replace("/", ""):
                score += 3
        if parsed.url:
            normalized_input_url = self._normalized_page_url(parsed.url)
            normalized_page_url = self._normalized_page_url(page_url)
            if normalized_input_url == normalized_page_url:
                score += 4
            elif normalized_input_url and normalized_input_url in normalized_page_url:
                score += 3
        if parsed.title:
            title_words = [word for word in (normalize_title(parsed.title) or "").split() if len(word) > 3]
            title_hits = sum(word in title or word in url for word in title_words[:8])
            if title_hits >= min(3, len(title_words[:8]) or 0):
                score += 3
            elif title_hits >= min(2, len(title_words[:8]) or 0):
                score += 2
        parsed_ids = self._identifier_tokens(parsed.url) | self._identifier_tokens(parsed.doi)
        if parsed_ids and parsed_ids.intersection(url_tokens):
            score += 2
        if parsed.publisher_hint and parsed.publisher_hint in url:
            score += 1
        return score

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

    def _materialize_download(self, download: Any, target: Path, existing_files: set[Path], timeout_seconds: int = 30) -> Path:
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            download.save_as(str(target))
        except Exception:
            pass
        if target.exists() and target.stat().st_size > 0:
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

        if target.exists() and target.stat().st_size > 0:
            return target
        raise RuntimeError("Chrome reported a download, but no non-empty PDF was materialized")

    def _find_native_download(self, existing_files: set[Path], suggested_name: str | None) -> Path | None:
        if suggested_name:
            candidate = self.config.download_dir / suggested_name
            if candidate.exists() and candidate.is_file() and candidate.stat().st_size > 0:
                return candidate
        for path in sorted(self.config.download_dir.glob("*"), key=lambda item: item.stat().st_mtime, reverse=True):
            resolved = path.resolve()
            if resolved in existing_files or path.suffix == ".crdownload" or not path.is_file():
                continue
            if path.stat().st_size > 0:
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
