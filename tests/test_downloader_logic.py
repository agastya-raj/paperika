from contextlib import contextmanager
from pathlib import Path

import pytest

from paperika.config import PaperikaConfig
from paperika.db import Database
import paperika.downloader as downloader_module
from paperika.downloader import Downloader
from paperika.models import ManualIntervention
from paperika.normalize import infer_input


class StubDownloader(Downloader):
    def __init__(self, *args, chrome_results=None, doi_result=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.chrome_results = list(chrome_results or [])
        self.doi_result = doi_result

    @contextmanager
    def _connected_local_browser(self):
        yield object()

    def _download_via_browser(self, browser, parsed, request_id, attempt_number):
        if not self.chrome_results:
            return None
        return self.chrome_results.pop(0)

    def _resolve_doi(self, doi):
        return self.doi_result


class CountingBrowserDownloader(StubDownloader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_session_entries = 0

    @contextmanager
    def _connected_local_browser(self):
        self.browser_session_entries += 1
        yield object()


class FailingArtifactDownloader(StubDownloader):
    def _capture_failure_artifact(self, request_id, attempt_number, message, parsed):
        artifact = self.config.screenshot_dir / f"request_{request_id}_attempt_{attempt_number}_failure.png"
        artifact.write_bytes(b"png")
        return ManualIntervention(
            reason=message,
            screenshot_path=str(artifact),
            page_title="Example page",
            current_url="https://example.org/paper",
            suggested_next_action="Open Chrome and retry",
        )


class FakePage:
    def __init__(self, url: str, title: str, link_candidates=None):
        self.url = url
        self._title = title
        self._link_candidates = list(link_candidates or [])

    def title(self):
        return self._title

    def eval_on_selector_all(self, selector, expression):
        return list(self._link_candidates)


class FakeDownloadContext:
    def __init__(self, download):
        self.value = download

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeLocator:
    def __init__(self, page):
        self.page = page

    @property
    def first(self):
        return self

    def count(self):
        return 1

    def click(self, timeout):
        self.page.click_attempts += 1
        if self.page.click_outcomes:
            outcome = self.page.click_outcomes.pop(0)
            if outcome:
                return None
        raise RuntimeError("download control not ready")


class FakePageContext:
    def __init__(self, page):
        self.page = page

    def new_cdp_session(self, page):
        return self.page


class FakeBrowserPage(FakePage):
    def __init__(self, url: str, title: str, download, link_candidates=None, goto_behaviors=None, click_outcomes=None):
        super().__init__(url, title, link_candidates=link_candidates)
        self.frames = []
        self._download = download
        self.context = FakePageContext(self)
        self.goto_behaviors = dict(goto_behaviors or {})
        self.click_outcomes = list(click_outcomes or [])
        self.wait_redirects = []
        self.click_attempts = 0
        self.wait_calls = 0
        self.goto_history = []

    def locator(self, selector):
        return FakeLocator(self)

    def expect_download(self, timeout):
        return FakeDownloadContext(self._download)

    def screenshot(self, path, full_page=False):
        Path(path).write_bytes(b"png")

    def wait_for_timeout(self, timeout):
        self.wait_calls += 1
        if self.wait_redirects:
            next_state = self.wait_redirects.pop(0)
            self.url = next_state.get("url", self.url)
            self._title = next_state.get("title", self._title)
            if "link_candidates" in next_state:
                self._link_candidates = list(next_state["link_candidates"])
            if "click_outcomes" in next_state:
                self.click_outcomes = list(next_state["click_outcomes"])
        return None

    def goto(self, url, wait_until="domcontentloaded", timeout=60000):
        self.goto_history.append(url)
        self.url = url
        behavior = self.goto_behaviors.get(url)
        if behavior:
            self.url = behavior.get("url", url)
            self._title = behavior.get("title", self._title)
            if "link_candidates" in behavior:
                self._link_candidates = list(behavior["link_candidates"])
            self.wait_redirects = list(behavior.get("wait_redirects", []))
            if "click_outcomes" in behavior:
                self.click_outcomes = list(behavior["click_outcomes"])
        else:
            self.wait_redirects = []
        return None

    def send(self, method, params):
        return None


class FakeDownload:
    suggested_filename = "downloaded.pdf"

    def __init__(self, payload: bytes = b"%PDF-1.4\nbody"):
        self.payload = payload

    def save_as(self, path):
        Path(path).write_bytes(self.payload)


class FakeBrowserContext:
    def __init__(self, page, new_page=None):
        self.pages = [page]
        self._new_page = new_page or page
        self.new_page_calls = 0

    def new_page(self):
        self.new_page_calls += 1
        if self._new_page not in self.pages:
            self.pages.append(self._new_page)
        return self._new_page


class FakeBrowser:
    def __init__(self, page, new_page=None):
        self.contexts = [FakeBrowserContext(page, new_page=new_page)]


class FakeChromium:
    def __init__(self, browser):
        self._browser = browser

    def connect_over_cdp(self, url):
        return self._browser


class FakePlaywrightRuntime:
    def __init__(self, browser):
        self.chromium = FakeChromium(browser)


class FakePlaywrightContextManager:
    def __init__(self, browser):
        self._runtime = FakePlaywrightRuntime(browser)

    def __enter__(self):
        return self._runtime

    def __exit__(self, exc_type, exc, tb):
        return False


def build_config(tmp_path: Path) -> PaperikaConfig:
    return PaperikaConfig(
        db_path=tmp_path / "papers.db",
        download_dir=tmp_path / "downloads",
        screenshot_dir=tmp_path / "shots",
        notification_dir=tmp_path / "events",
    )


def test_download_fallback_prefers_local_chrome(tmp_path: Path):
    config = build_config(tmp_path)
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    downloader = StubDownloader(config, db, chrome_results=[str(config.download_dir / "x.pdf")])

    parsed = infer_input("https://example.org/paper.pdf")
    paper_id = downloader._ensure_paper_record(parsed)
    outcome = downloader._download_with_fallback(parsed, request_id=1, paper_id=paper_id, attempt_number=1)
    assert outcome.local_pdf_path.endswith("x.pdf")
    assert outcome.status == "downloaded"


def test_download_fallback_uses_doi_resolution(tmp_path: Path):
    config = build_config(tmp_path)
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    downloader = StubDownloader(
        config,
        db,
        chrome_results=[str(config.download_dir / "resolved.pdf")],
        doi_result="https://example.org/resolved.pdf",
    )

    parsed = infer_input("10.1000/test-doi")
    paper_id = downloader._ensure_paper_record(parsed)
    outcome = downloader._download_with_fallback(parsed, request_id=1, paper_id=paper_id, attempt_number=1)
    assert outcome.local_pdf_path.endswith("resolved.pdf")


def test_resolve_doi_uses_browserish_headers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config = build_config(tmp_path)
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    downloader = Downloader(config, db)
    captured = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def geturl(self):
            return "https://opg.optica.org/abstract.cfm?URI=OFC-2025-M2C.5"

    def fake_urlopen(request, timeout=20):
        captured["accept"] = request.headers.get("Accept")
        captured["user_agent"] = request.headers.get("User-agent")
        return FakeResponse()

    monkeypatch.setattr(downloader_module, "urlopen", fake_urlopen)
    resolved = downloader._resolve_doi("10.1364/ofc.2025.m2c.5")
    assert resolved == "https://opg.optica.org/abstract.cfm?URI=OFC-2025-M2C.5"
    assert "paperika/0.1.0" not in (captured["user_agent"] or "")
    assert "Chrome" in (captured["user_agent"] or "")
    assert "text/html" in (captured["accept"] or "")


def test_materialize_url_download_writes_pdf_bytes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config = build_config(tmp_path)
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    downloader = Downloader(config, db)
    target = config.download_dir / "remote.pdf"

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return b"%PDF-1.4\nbody"

    monkeypatch.setattr(downloader_module, "urlopen", lambda request, timeout=30: FakeResponse())
    result = downloader._materialize_url_download("https://example.org/direct.pdf", target)
    assert result == target
    assert target.read_bytes().startswith(b"%PDF")
    assert downloader._is_downloadable_pdf_url("https://opg.optica.org/directpdfaccess/token/file.pdf") is True


def test_download_via_browser_handles_jocn_fulltext_pdf_redirect(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config = build_config(tmp_path)
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    downloader = Downloader(config, db)
    fulltext_url = "https://opg.optica.org/jocn/fulltext.cfm?uri=jocn-17-9-D106"
    pdf_candidate = "https://opg.optica.org/jocn/viewmedia.cfm?uri=jocn-17-9-D106&seq=0"
    direct_pdf_url = "https://opg.optica.org/directpdfaccess/token/jocn-17-9-d106.pdf?download=1"
    parsed = infer_input("Generalized few-shot transfer learning architecture for modeling the EDFA gain spectrum 10.1364/JOCN.560987")
    parsed.url = fulltext_url

    page = FakeBrowserPage(
        fulltext_url,
        "Generalized few-shot transfer learning architecture for modeling the EDFA gain spectrum",
        FakeDownload(),
        link_candidates=[
            {"href": pdf_candidate, "text": "PDF Article", "aria": "Pdf icon", "title": ""},
        ],
        goto_behaviors={
            pdf_candidate: {
                "url": pdf_candidate,
                "title": "PDF Article",
                "wait_redirects": [
                    {"url": pdf_candidate, "title": "PDF Article"},
                    {"url": direct_pdf_url, "title": "Direct PDF"},
                ],
            }
        },
    )
    browser = FakeBrowser(page)
    captured = {}

    def fake_materialize(url, target):
        captured["url"] = url
        target.write_bytes(b"%PDF-1.4\nbody")
        return target

    monkeypatch.setattr(downloader, "_materialize_url_download", fake_materialize)
    monkeypatch.setattr(
        downloader,
        "_verify_downloaded_pdf_identity",
        lambda parsed, pdf_path: downloader_module.PdfIdentityCheck(True, "ok", parsed.title, parsed.doi),
    )

    result = downloader._download_via_browser(browser, parsed, request_id=1, attempt_number=1)

    assert result is not None
    assert Path(result).exists()
    assert captured["url"] == direct_pdf_url
    assert page.goto_history == [pdf_candidate]
    assert page.wait_calls >= 2


def test_download_via_browser_retries_after_pdf_candidate_navigation_settles(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config = build_config(tmp_path)
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    downloader = Downloader(config, db)
    fulltext_url = "https://opg.optica.org/jocn/fulltext.cfm?uri=jocn-17-9-D106"
    pdf_candidate = "https://opg.optica.org/jocn/viewmedia.cfm?uri=jocn-17-9-D106&seq=0"
    parsed = infer_input("Generalized few-shot transfer learning architecture for modeling the EDFA gain spectrum 10.1364/JOCN.560987")
    parsed.url = fulltext_url

    page = FakeBrowserPage(
        fulltext_url,
        "Generalized few-shot transfer learning architecture for modeling the EDFA gain spectrum",
        FakeDownload(),
        link_candidates=[
            {"href": pdf_candidate, "text": "PDF Article", "aria": "Pdf icon", "title": ""},
        ],
        goto_behaviors={
            pdf_candidate: {
                "url": pdf_candidate,
                "title": "PDF viewer",
                "click_outcomes": [False],
                "wait_redirects": [
                    {"url": pdf_candidate, "title": "PDF viewer", "click_outcomes": [False]},
                    {"url": pdf_candidate, "title": "PDF viewer", "click_outcomes": [True]},
                ],
            }
        },
    )
    browser = FakeBrowser(page)
    captured = {}

    def fake_materialize_clicked(target, existing_files, timeout_seconds=30):
        captured["target"] = target
        target.write_bytes(b"%PDF-1.4\nbody")
        return target

    monkeypatch.setattr(downloader, "_materialize_clicked_download", fake_materialize_clicked)
    monkeypatch.setattr(
        downloader,
        "_verify_downloaded_pdf_identity",
        lambda parsed, pdf_path: downloader_module.PdfIdentityCheck(True, "ok", parsed.title, parsed.doi),
    )

    result = downloader._download_via_browser(browser, parsed, request_id=2, attempt_number=1)

    assert result is not None
    assert Path(result).exists()
    assert captured["target"].exists()
    assert page.goto_history == [pdf_candidate]
    assert page.click_attempts >= 2
    assert page.wait_calls >= 2


def test_download_via_browser_falls_back_to_click_when_direct_pdf_materialization_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config = build_config(tmp_path)
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    downloader = Downloader(config, db)
    direct_pdf_url = "https://opg.optica.org/directpdfaccess/token/jocn-17-9-d106.pdf?download=1"
    parsed = infer_input("Generalized few-shot transfer learning architecture for modeling the EDFA gain spectrum 10.1364/JOCN.560987")
    parsed.url = direct_pdf_url

    page = FakeBrowserPage(
        direct_pdf_url,
        "Direct PDF",
        FakeDownload(),
        click_outcomes=[True],
    )
    browser = FakeBrowser(page)
    clicked = {}

    monkeypatch.setattr(
        downloader,
        "_materialize_url_download",
        lambda url, target: (_ for _ in ()).throw(RuntimeError("raw fetch requires browser session")),
    )

    def fake_materialize_clicked(target, existing_files, timeout_seconds=30):
        clicked["target"] = target
        target.write_bytes(b"%PDF-1.4\nbody")
        return target

    monkeypatch.setattr(downloader, "_materialize_clicked_download", fake_materialize_clicked)
    monkeypatch.setattr(
        downloader,
        "_verify_downloaded_pdf_identity",
        lambda parsed, pdf_path: downloader_module.PdfIdentityCheck(True, "ok", parsed.title, parsed.doi),
    )

    result = downloader._download_via_browser(browser, parsed, request_id=3, attempt_number=1)

    assert result is not None
    assert Path(result).exists()
    assert clicked["target"].exists()
    assert page.click_attempts >= 1


def test_download_fallback_reuses_one_browser_session_across_doi_resolution(tmp_path: Path):
    config = build_config(tmp_path)
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    downloader = CountingBrowserDownloader(
        config,
        db,
        chrome_results=[str(config.download_dir / "resolved.pdf")],
        doi_result="https://example.org/resolved.pdf",
    )

    parsed = infer_input("10.1000/test-doi")
    paper_id = downloader._ensure_paper_record(parsed)
    outcome = downloader._download_with_fallback(parsed, request_id=1, paper_id=paper_id, attempt_number=1)
    assert outcome.local_pdf_path.endswith("resolved.pdf")
    assert downloader.browser_session_entries == 1


def test_process_request_moves_to_retrying_on_failure_and_persists_png_artifact(tmp_path: Path):
    config = build_config(tmp_path)
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()

    downloader = FailingArtifactDownloader(config, db, chrome_results=[None], doi_result=None)
    queued = downloader.enqueue("Some missing paper 10.1000/test-fail")
    outcome = downloader.process_request(queued["request_id"])
    assert outcome.status == "retrying"
    assert outcome.manual is not None
    assert outcome.manual.screenshot_path is not None
    assert outcome.manual.screenshot_path.endswith(".png")
    assert outcome.notification_events[0].event_type == "first_failure"

    request_row = db.get_request(queued["request_id"])
    assert request_row["manual_screenshot_path"].endswith(".png")
    with db.connect() as conn:
        attempt = conn.execute("SELECT * FROM paper_attempts WHERE request_id = ?", (queued["request_id"],)).fetchone()
    assert attempt["screenshot_path"].endswith(".png")
    assert Path(outcome.manual.screenshot_path).exists()


def test_completed_request_is_not_reprocessed(tmp_path: Path):
    config = build_config(tmp_path)
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    pdf_path = config.download_dir / "existing.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n")

    downloader = StubDownloader(config, db, chrome_results=[])
    parsed = infer_input("Known paper")
    paper_id = downloader._ensure_paper_record(parsed)
    db.mark_paper_downloaded(paper_id, str(pdf_path))
    first = downloader.enqueue("Known paper")
    outcome = downloader.process_request(first["request_id"])
    assert outcome.status == "completed_deduped"
    assert outcome.attempt_number == 0


def test_manual_request_is_guarded_from_reprocessing(tmp_path: Path):
    config = build_config(tmp_path)
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()

    downloader = StubDownloader(config, db, chrome_results=[])
    queued = downloader.enqueue("10.1000/manual-guard")
    db.update_request_status(
        queued["request_id"],
        "manual_intervention",
        attempt_count=2,
        manual=ManualIntervention(reason="Need login", screenshot_path="/tmp/failure.png"),
    )

    outcome = downloader.process_request(queued["request_id"])
    assert outcome.status == "manual_intervention"
    assert "explicit retry" in outcome.message
    assert outcome.attempt_number == 2


def test_target_pdf_path_sanitizes_doi(tmp_path: Path):
    config = build_config(tmp_path)
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    downloader = StubDownloader(config, db)

    parsed = infer_input("10.1000/test-doi")
    target = downloader._target_pdf_path(parsed, paper_id=None)
    assert target.parent == config.download_dir
    assert target.name == "10_1000_test_doi.pdf"


def test_page_matching_handles_doi_only_inputs_with_identifier_overlap(tmp_path: Path):
    config = build_config(tmp_path)
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    downloader = StubDownloader(config, db)

    parsed = infer_input("10.1145/3544548.3580875")
    assert downloader._page_matches(
        "https://dl.acm.org/doi/pdf/10.1145/3544548.3580875",
        "PDF viewer",
        parsed,
    ) is True


def test_find_matching_page_rejects_unrelated_open_pdf_tab(tmp_path: Path):
    config = build_config(tmp_path)
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    downloader = StubDownloader(config, db)

    parsed = infer_input("Advanced Optical Link Tomography for Optical Network Monitoring 10.1364/ofc.2025.m2c.5")
    pages = [
        FakePage(
            "https://ieeexplore.ieee.org/stampPDF/getPDF.jsp?tp=&arnumber=1234567",
            "Optical network monitoring in IEEE Xplore PDF viewer",
        )
    ]

    assert downloader._find_matching_page(pages, parsed) is None


def test_find_pdf_link_candidate_prefers_pdf_article_links(tmp_path: Path):
    config = build_config(tmp_path)
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    downloader = StubDownloader(config, db)
    page = FakePage(
        "https://opg.optica.org/abstract.cfm?URI=OFC-2025-M2C.5",
        "Advanced Optical Link Tomography for Optical Network Monitoring",
        link_candidates=[
            {"href": "https://example.org/supplement", "text": "Supplementary material", "aria": "", "title": ""},
            {"href": "https://opg.optica.org/viewmedia.cfm?uri=OFC-2025-M2C.5&seq=0", "text": "PDF Article", "aria": "Pdf icon", "title": ""},
        ],
    )

    assert downloader._find_pdf_link_candidate(page) == "https://opg.optica.org/viewmedia.cfm?uri=OFC-2025-M2C.5&seq=0"


def test_page_match_requires_strong_evidence_for_reuse(tmp_path: Path):
    config = build_config(tmp_path)
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    downloader = StubDownloader(config, db)

    doi_parsed = infer_input("Advanced Optical Link Tomography for Optical Network Monitoring 10.1364/ofc.2025.m2c.5")
    weak = downloader._evaluate_page_match(
        "https://ieeexplore.ieee.org/document/1234567",
        "Optical network monitoring for links",
        doi_parsed,
    )
    assert weak.reuse_allowed is False

    doi_url_match = downloader._evaluate_page_match(
        "https://doi.org/10.1364/ofc.2025.m2c.5",
        "PDF viewer",
        doi_parsed,
    )
    assert doi_url_match.reuse_allowed is True

    exact_url_parsed = infer_input("https://dl.acm.org/doi/pdf/10.1145/3544548.3580875")
    exact_url_match = downloader._evaluate_page_match(
        "https://dl.acm.org/doi/pdf/10.1145/3544548.3580875",
        "ACM Digital Library",
        exact_url_parsed,
    )
    assert exact_url_match.reuse_allowed is True

    publisher_only = downloader._evaluate_page_match(
        "https://ieeexplore.ieee.org/document/7654321",
        "IEEE Xplore",
        infer_input("https://ieeexplore.ieee.org/document/9999999"),
    )
    assert publisher_only.reuse_allowed is False

    title_only = downloader._evaluate_page_match(
        "https://example.org/paper",
        "Cascaded Learning Tomography Measurements",
        infer_input("Cascaded Learning Tomography Measurements"),
    )
    assert title_only.reuse_allowed is True


def test_text_contains_doi_requires_exact_token(tmp_path: Path):
    config = build_config(tmp_path)
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    downloader = StubDownloader(config, db)

    assert downloader._text_contains_doi("https://doi.org/10.1234/abcd", "10.1234/abcd") is True
    assert downloader._text_contains_doi("https://doi.org/10.1234/abcde", "10.1234/abcd") is False
    assert downloader._text_contains_doi("doi:10.1234/abcd.", "10.1234/abcd") is True


def test_verify_downloaded_pdf_identity_accepts_matching_title_without_doi(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config = build_config(tmp_path)
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    downloader = StubDownloader(config, db)
    pdf_path = config.download_dir / "matched.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n")
    parsed = infer_input("Cascaded Learning Tomography Measurements")

    monkeypatch.setattr(
        downloader,
        "_extract_pdf_identity",
        lambda path: ("Cascaded Learning Tomography Measurements", None),
    )

    result = downloader._verify_downloaded_pdf_identity(parsed, pdf_path)
    assert result.ok is True


def test_verify_downloaded_pdf_identity_rejects_title_only_match_for_doi_known_request(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config = build_config(tmp_path)
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    downloader = StubDownloader(config, db)
    pdf_path = config.download_dir / "matched.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n")
    parsed = infer_input("Cascaded Learning Tomography Measurements 10.1000/test-doi")

    monkeypatch.setattr(
        downloader,
        "_extract_pdf_identity",
        lambda path: ("Cascaded Learning Tomography Measurements", None),
    )
    monkeypatch.setattr(downloader, "_extract_pdf_text", lambda path, max_chars=20000: "")

    result = downloader._verify_downloaded_pdf_identity(parsed, pdf_path)
    assert result.ok is False
    assert "strong title match in extracted text" in result.reason


def test_verify_downloaded_pdf_identity_accepts_strong_extracted_text_for_doi_known_request(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config = build_config(tmp_path)
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    downloader = StubDownloader(config, db)
    pdf_path = config.download_dir / "matched.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n")
    parsed = infer_input("Advanced Optical Link Tomography for Optical Network Monitoring 10.1364/ofc.2025.m2c.5")

    monkeypatch.setattr(downloader, "_extract_pdf_identity", lambda path: (None, None))
    monkeypatch.setattr(
        downloader,
        "_extract_pdf_text",
        lambda path, max_chars=20000: "Advanced Optical Link Tomography for Optical Network Monitoring by Alix May, Fabien Boitier, Patricia Layec",
    )

    result = downloader._verify_downloaded_pdf_identity(parsed, pdf_path)
    assert result.ok is True
    assert "extracted PDF text" in result.reason


def test_download_with_fallback_rejects_pdf_identity_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config = build_config(tmp_path)
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    downloader = StubDownloader(config, db)

    mismatch_path = config.download_dir / "wrong.pdf"
    mismatch_path.write_bytes(b"%PDF-1.4\n")
    parsed = infer_input("Advanced Optical Link Tomography for Optical Network Monitoring 10.1364/ofc.2025.m2c.5")
    paper_id = downloader._ensure_paper_record(parsed)

    def raise_mismatch(browser, parsed, request_id, attempt_number):
        identity = downloader._verify_downloaded_pdf_identity(parsed, mismatch_path)
        if not identity.ok:
            raise RuntimeError(f"Downloaded PDF identity mismatch: {identity.reason}; pdf_path={mismatch_path}")
        return str(mismatch_path)

    monkeypatch.setattr(
        downloader,
        "_extract_pdf_identity",
        lambda path: ("Different Paper Title", "10.9999/wrong-paper"),
    )
    monkeypatch.setattr(downloader, "_download_via_browser", raise_mismatch)

    with pytest.raises(RuntimeError, match="Downloaded PDF identity mismatch"):
        downloader._download_with_fallback(parsed, request_id=1, paper_id=paper_id, attempt_number=1)


def test_process_request_does_not_mark_downloaded_on_real_pdf_identity_rejection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config = build_config(tmp_path)
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()

    downloader = Downloader(config, db)
    queued = downloader.enqueue("Advanced Optical Link Tomography for Optical Network Monitoring 10.1364/ofc.2025.m2c.5")

    monkeypatch.setattr(
        downloader,
        "_capture_failure_artifact",
        lambda request_id, attempt_number, message, parsed: ManualIntervention(
            reason=message,
            screenshot_path=str(config.screenshot_dir / f"request_{request_id}_attempt_{attempt_number}_failure.png"),
            page_title="Example page",
            current_url="https://example.org/paper",
            suggested_next_action="Open Chrome and retry",
        ),
    )

    mismatch_path = config.download_dir / "wrong.pdf"
    mismatch_path.write_bytes(b"%PDF-1.4\n")

    fake_page = FakeBrowserPage(
        "https://doi.org/10.1364/ofc.2025.m2c.5",
        "Advanced Optical Link Tomography for Optical Network Monitoring",
        FakeDownload(),
        click_outcomes=[True],
    )
    monkeypatch.setattr(
        downloader_module,
        "sync_playwright",
        lambda: FakePlaywrightContextManager(FakeBrowser(fake_page)),
    )
    monkeypatch.setattr(
        downloader,
        "_find_matching_page",
        lambda pages, parsed: fake_page,
    )
    monkeypatch.setattr(
        downloader,
        "_materialize_clicked_download",
        lambda target, existing_files, timeout_seconds=30: mismatch_path,
    )
    monkeypatch.setattr(
        downloader,
        "_extract_pdf_identity",
        lambda path: ("Advanced Optical Link Tomography for Optical Network Monitoring", None),
    )

    outcome = downloader.process_request(queued["request_id"])
    assert outcome.status == "retrying"
    assert outcome.manual is not None
    assert outcome.manual.screenshot_path.endswith(".png")
    assert "identity mismatch" in outcome.message.lower()
    assert "observed_doi=n/a" in outcome.message
    assert "strong title match in extracted text" in outcome.message

    request_row = db.get_request(queued["request_id"])
    assert request_row["status"] == "retrying"
    with db.connect() as conn:
        paper = conn.execute("SELECT * FROM papers WHERE id = ?", (queued["paper_id"],)).fetchone()
        attempt = conn.execute("SELECT * FROM paper_attempts WHERE request_id = ?", (queued["request_id"],)).fetchone()
    assert paper["verified_pdf"] == 0
    assert paper["local_pdf_path"] is None
    assert attempt["message"] is not None
    assert "identity mismatch" in attempt["message"].lower()
    assert "strong title match in extracted text" in attempt["message"]
    assert attempt["screenshot_path"].endswith(".png")


def test_process_request_does_not_mark_downloaded_on_pdf_identity_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config = build_config(tmp_path)
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()

    downloader = FailingArtifactDownloader(config, db)
    queued = downloader.enqueue("Advanced Optical Link Tomography for Optical Network Monitoring 10.1364/ofc.2025.m2c.5")

    mismatch_path = config.download_dir / "wrong.pdf"
    mismatch_path.write_bytes(b"%PDF-1.4\n")

    def raise_mismatch(browser, parsed, request_id, attempt_number):
        identity = downloader._verify_downloaded_pdf_identity(parsed, mismatch_path)
        if not identity.ok:
            raise RuntimeError(
                "Downloaded PDF identity mismatch: "
                f"target_doi={parsed.doi}; observed_doi={identity.observed_doi}; "
                f"observed_title={identity.observed_title}; pdf_path={mismatch_path}; reason={identity.reason}"
            )
        return str(mismatch_path)

    monkeypatch.setattr(
        downloader,
        "_extract_pdf_identity",
        lambda path: ("Completely Different Paper", "10.9999/wrong-paper"),
    )
    monkeypatch.setattr(downloader, "_download_via_browser", raise_mismatch)

    outcome = downloader.process_request(queued["request_id"])
    assert outcome.status == "retrying"
    assert outcome.manual is not None
    assert outcome.manual.screenshot_path.endswith(".png")
    assert "identity mismatch" in outcome.message.lower()
    assert "observed_doi=10.9999/wrong-paper" in outcome.message

    request_row = db.get_request(queued["request_id"])
    assert request_row["status"] == "retrying"
    with db.connect() as conn:
        paper = conn.execute("SELECT * FROM papers WHERE id = ?", (queued["paper_id"],)).fetchone()
        attempt = conn.execute("SELECT * FROM paper_attempts WHERE request_id = ?", (queued["request_id"],)).fetchone()
    assert paper["verified_pdf"] == 0
    assert paper["local_pdf_path"] is None
    assert attempt["message"] is not None
    assert "identity mismatch" in attempt["message"].lower()
    assert attempt["screenshot_path"].endswith(".png")


def test_materialize_download_uses_native_file_when_save_as_is_empty(tmp_path: Path):
    config = build_config(tmp_path)
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    downloader = StubDownloader(config, db)

    native = config.download_dir / "native.pdf"
    native.write_bytes(b"%PDF-1.7\nbody")
    target = config.download_dir / "slugified.pdf"

    class FakeDownload:
        suggested_filename = "native.pdf"

        def save_as(self, path):
            Path(path).write_bytes(b"")

    result = downloader._materialize_download(FakeDownload(), target, existing_files=set(), timeout_seconds=1)
    assert result == target
    assert target.read_bytes().startswith(b"%PDF")


def test_materialize_clicked_download_rejects_non_pdf_candidates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config = build_config(tmp_path)
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()
    downloader = StubDownloader(config, db)

    html_candidate = config.download_dir / "login.pdf"
    html_candidate.write_text("<html>sign in</html>", encoding="utf-8")
    pdf_candidate = config.download_dir / "real.pdf"
    pdf_candidate.write_bytes(b"%PDF-1.7\nbody")
    target = config.download_dir / "slugified.pdf"
    candidates = iter([html_candidate, pdf_candidate])

    monkeypatch.setattr(
        downloader,
        "_find_native_download",
        lambda existing_files, suggested_name=None: next(candidates, None),
    )

    result = downloader._materialize_clicked_download(target, existing_files=set(), timeout_seconds=1)

    assert result == target
    assert target.read_bytes().startswith(b"%PDF")
