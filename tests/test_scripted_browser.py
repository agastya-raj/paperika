"""AGA-339 Tier-2 scripted-browser tests. Zero live browser, zero network.

Pure logic (pick_pdf_anchor / is_challenge_html / sanitizer) is unit-tested
directly. attempt_scripted_fetch is driven through a duck-typed fake page +
context injected by monkeypatching ``_connect_cdp``, so the real page-lifecycle
(new_page + finally-close) and _safe_close run against the fake.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

import pytest

from paperika.bridge import direct_fetch
from paperika.bridge import scripted_browser as sb

BIG_PDF = b"%PDF-1.5\n" + b"x" * 20_000
HTML_BODY = b"<html><body>not a pdf</body></html>"


@pytest.fixture(autouse=True)
def _public_dns(monkeypatch):
    """pick_pdf_anchor now gates each candidate through the shared SSRF guard,
    which resolves the host; map the test publisher hostnames to a public IP so
    the guard admits them without touching real DNS. IP-literal / file:// / other
    internal targets are refused without a lookup, so they stay blocked."""
    monkeypatch.setattr(direct_fetch, "_resolve_host", lambda host: ["93.184.216.34"])


# --------------------------------------------------------------- pure logic ---


def test_sanitizer_replaces_doi_separators():
    assert sb.sanitize_doi("10.1364/jocn.560987") == "10.1364_jocn.560987"
    assert sb.sanitize_doi("10.1145/3544548.3580875") == "10.1145_3544548.3580875"
    # every unsafe char collapses to '_', dots/dashes/underscores survive.
    assert sb.sanitize_doi("10.1000/a b:c?d") == "10.1000_a_b_c_d"


def test_pick_anchor_toolbar_pdf_chosen():
    anchors = [("/article/12345/pdf", "PDF")]
    assert (
        sb.pick_pdf_anchor(anchors, "https://pub.example.org/article/12345")
        == "https://pub.example.org/article/12345/pdf"
    )


def test_pick_anchor_resolves_relative_href():
    anchors = [("pdf", "Download PDF")]
    assert (
        sb.pick_pdf_anchor(anchors, "https://pub.example.org/article/abc")
        == "https://pub.example.org/article/pdf"
    )


def test_pick_anchor_text_marker_without_pdf_in_href():
    # opg.optica.org style: href has no "pdf", the anchor text does.
    anchors = [
        ("https://x.org/supplement", "Supplementary material"),
        ("https://opg.optica.org/viewmedia.cfm?uri=jocn-17-9-D106&seq=0", "PDF Article"),
    ]
    assert (
        sb.pick_pdf_anchor(anchors, "https://opg.optica.org/abstract.cfm?URI=jocn-17-9-D106")
        == "https://opg.optica.org/viewmedia.cfm?uri=jocn-17-9-D106&seq=0"
    )


def test_pick_anchor_excludes_g_suffix_figure_decoy():
    # -g001 figure graphic must be rejected even though href ends .pdf / text says PDF.
    anchors = [("https://x.org/jocn-17-9-g001.pdf", "View PDF")]
    assert sb.pick_pdf_anchor(anchors, "https://x.org/") is None


def test_pick_anchor_excludes_download_full_size_pdf_figure_link():
    anchors = [("https://x.org/figs/g009", "Download Full Size | PDF")]
    assert sb.pick_pdf_anchor(anchors, "https://x.org/") is None


def test_pick_anchor_real_toolbar_pdf_wins_over_figure_decoys():
    anchors = [
        ("https://x.org/figure/g001", "Download Full Size | PDF"),
        ("https://x.org/media-g002.pdf", "View PDF"),  # figure decoy via -g002
        ("https://x.org/article/pdf", "PDF"),  # the real one
    ]
    assert sb.pick_pdf_anchor(anchors, "https://x.org/") == "https://x.org/article/pdf"


def test_pick_anchor_first_in_dom_order_among_valid():
    anchors = [
        ("https://x.org/a/pdf", "PDF"),
        ("https://x.org/b/pdf", "Download PDF"),
    ]
    assert sb.pick_pdf_anchor(anchors, "https://x.org/") == "https://x.org/a/pdf"


def test_pick_anchor_skips_non_navigational_hrefs():
    anchors = [
        ("#", "PDF"),
        ("javascript:void(0)", "PDF"),
        ("https://x.org/article/pdf", "PDF"),
    ]
    assert sb.pick_pdf_anchor(anchors, "https://x.org/") == "https://x.org/article/pdf"


def test_pick_anchor_none_when_no_marker():
    anchors = [
        ("https://x.org/supplement", "Supplementary material"),
        ("https://x.org/cite", "Export citation"),
    ]
    assert sb.pick_pdf_anchor(anchors, "https://x.org/") is None


def test_pick_anchor_rejects_internal_and_file_anchors():
    """Finding 3 (MEDIUM): a file:// path or an internal-host anchor from the
    untrusted page DOM must NOT be returned for navigation, even with a PDF
    marker; a public publisher PDF link still wins."""
    base = "https://opg.optica.org/jocn/fulltext.cfm?uri=x"
    # a file:// "PDF" anchor is refused
    assert sb.pick_pdf_anchor([("file:///etc/hosts.pdf", "PDF")], base) is None
    # the CDP control port disguised with a 'pdf' query token is refused
    assert sb.pick_pdf_anchor(
        [("http://127.0.0.1:9224/json/version?x=pdf", "PDF")], base
    ) is None
    # an internal anchor is skipped, and a later public publisher PDF link is chosen
    assert sb.pick_pdf_anchor(
        [
            ("http://127.0.0.1:9224/json/version?x=pdf", "PDF"),
            ("/jocn/viewmedia.cfm?uri=x&seq=0", "PDF Article"),
        ],
        base,
    ) == "https://opg.optica.org/jocn/viewmedia.cfm?uri=x&seq=0"


def test_is_challenge_html_detects_bot_walls():
    assert sb.is_challenge_html("<title>Just a moment...</title>") is True
    assert sb.is_challenge_html("please solve the CAPTCHA to continue") is True
    assert sb.is_challenge_html("Checking your browser before accessing") is True
    assert sb.is_challenge_html('<div class="cf-browser-verification">') is True


def test_is_challenge_html_benign_page_is_false():
    assert sb.is_challenge_html("<html><body>Full text of the article. We use cookies.</body></html>") is False
    assert sb.is_challenge_html("") is False


# ----------------------------------------------------------- fake page/ctx ---


class _FakeResponse:
    def __init__(self, body: bytes) -> None:
        self._body = body

    async def body(self) -> bytes:
        return self._body


class _FakeRequest:
    def __init__(self, request_map: dict[str, bytes]) -> None:
        self._map = request_map
        self.calls: list[str] = []

    async def get(self, url: str) -> _FakeResponse:
        self.calls.append(url)
        return _FakeResponse(self._map.get(url, b""))


class FakePage:
    """Duck-typed stand-in for a Playwright page. ``redirects`` maps a URL to the
    URL it becomes after a settle (models a delivery interstitial)."""

    def __init__(
        self,
        *,
        url: str,
        anchors: tuple[tuple[str, str], ...] = (),
        contents: list[str] | None = None,
        request_map: dict[str, bytes] | None = None,
        redirects: dict[str, str] | None = None,
        cookie_button: str | None = None,
        raise_on_goto: Exception | None = None,
    ) -> None:
        self.url = url
        self._anchors = [list(a) for a in anchors]
        self._contents = list(contents) if contents is not None else None
        self._request_map = request_map or {}
        self.request = _FakeRequest(self._request_map)
        self._redirects = dict(redirects or {})
        self._cookie_button = cookie_button
        self._raise_on_goto = raise_on_goto
        self.closed = False
        self.goto_calls: list[str] = []
        self.click_calls: list[str] = []
        self.wait_calls = 0

    async def goto(self, url: str, wait_until: str = "domcontentloaded") -> None:
        self.goto_calls.append(url)
        if self._raise_on_goto is not None:
            raise self._raise_on_goto
        self.url = url

    async def wait_for_timeout(self, ms: int) -> None:
        self.wait_calls += 1
        if self.url in self._redirects:
            self.url = self._redirects.pop(self.url)

    async def content(self) -> str:
        if self._contents:
            return self._contents.pop(0)
        return "<html><body>article full text</body></html>"

    async def eval_on_selector_all(self, selector: str, expression: str) -> list[list[str]]:
        return [list(a) for a in self._anchors]

    async def click(self, selector: str, timeout: int | None = None) -> None:
        self.click_calls.append(selector)
        if self._cookie_button is not None and selector == self._cookie_button:
            return None
        raise RuntimeError("no such element")

    async def close(self) -> None:
        self.closed = True


class _FakeContext:
    def __init__(self, page: FakePage) -> None:
        self._page = page
        self.new_page_calls = 0

    async def new_page(self) -> FakePage:
        self.new_page_calls += 1
        return self._page


def _patch_connection(monkeypatch: pytest.MonkeyPatch, page: FakePage) -> None:
    @asynccontextmanager
    async def _fake_connect(cdp_http_url: str):
        yield _FakeContext(page)

    monkeypatch.setattr(sb, "_connect_cdp", _fake_connect)


def _run(page: FakePage, tmp_path: Path, *, doi: str = "10.1364/jocn.560987", budget: float = 45.0):
    return asyncio.run(
        sb.attempt_scripted_fetch(
            doi=doi,
            title="Some Paper",
            start_url="https://opg.optica.org/abstract.cfm?URI=jocn-17-9-D106",
            download_dir=tmp_path / "downloads",
            cdp_http_url="http://127.0.0.1:9224",
            budget_seconds=budget,
        )
    )


# --------------------------------------------------- attempt_scripted_fetch ---


def test_happy_path_writes_verified_pdf(tmp_path, monkeypatch):
    media_url = "https://opg.optica.org/directpdfaccess/tok/jocn-17-9-d106.pdf"
    page = FakePage(
        url="https://opg.optica.org/abstract.cfm?URI=jocn-17-9-D106",
        anchors=(("https://opg.optica.org/viewmedia.cfm?uri=jocn-17-9-D106&seq=0", "PDF Article"),),
        redirects={"https://opg.optica.org/viewmedia.cfm?uri=jocn-17-9-D106&seq=0": media_url},
        request_map={media_url: BIG_PDF},
    )
    _patch_connection(monkeypatch, page)
    result = _run(page, tmp_path)

    assert result.kind == "downloaded"
    assert result.file_path == str(tmp_path / "downloads" / "10.1364_jocn.560987.pdf")
    assert Path(result.file_path).read_bytes() == BIG_PDF
    assert result.final_url == media_url
    assert page.closed is True  # page closed on the success path too


def test_interstitial_html_then_direct_pdf_retry(tmp_path, monkeypatch):
    anchor = "https://opg.optica.org/viewmedia.cfm?uri=jocn-17-9-D106&seq=0"
    media_url = "https://opg.optica.org/interstitial/opening-shortly"
    page = FakePage(
        url="https://opg.optica.org/abstract.cfm?URI=jocn-17-9-D106",
        anchors=((anchor, "PDF Article"),),
        redirects={anchor: media_url},
        # settled media URL serves HTML; the anchor URL itself serves the PDF.
        request_map={media_url: HTML_BODY, anchor: BIG_PDF},
    )
    _patch_connection(monkeypatch, page)
    result = _run(page, tmp_path)

    assert result.kind == "downloaded"
    assert result.final_url == anchor  # retry against the anchor URL won
    assert Path(result.file_path).read_bytes() == BIG_PDF
    assert page.request.calls == [media_url, anchor]  # media first, then one retry
    assert page.closed is True


def test_bot_wall_on_landing(tmp_path, monkeypatch):
    start_url = "https://opg.optica.org/abstract.cfm?URI=jocn-17-9-D106"  # matches _run
    page = FakePage(
        url="https://pub.example.org/paper",
        anchors=(("https://pub.example.org/pdf", "PDF"),),
        contents=["<title>Just a moment...</title> checking your browser"],
    )
    _patch_connection(monkeypatch, page)
    result = _run(page, tmp_path)

    assert result.kind == "wall"
    assert result.final_url == start_url  # walled on the landing page
    assert page.goto_calls == [start_url]  # never navigated onward
    assert page.closed is True


def test_no_anchor_is_no_pdf(tmp_path, monkeypatch):
    page = FakePage(
        url="https://pub.example.org/paper",
        anchors=(("https://pub.example.org/cite", "Export citation"),),
    )
    _patch_connection(monkeypatch, page)
    result = _run(page, tmp_path)

    assert result.kind == "no_pdf"
    assert page.closed is True
    assert list((tmp_path / "downloads").glob("*.pdf")) == []  # nothing written


def test_anchor_but_non_pdf_bytes_is_no_pdf(tmp_path, monkeypatch):
    anchor = "https://pub.example.org/pdf"
    page = FakePage(
        url="https://pub.example.org/paper",
        anchors=((anchor, "PDF"),),
        request_map={anchor: HTML_BODY},  # no redirect: media_url == anchor, no retry
    )
    _patch_connection(monkeypatch, page)
    result = _run(page, tmp_path)

    assert result.kind == "no_pdf"
    assert page.request.calls == [anchor]  # media==anchor ⇒ no pointless retry
    assert page.closed is True


def test_too_small_pdf_rejected(tmp_path, monkeypatch):
    anchor = "https://pub.example.org/pdf"
    page = FakePage(
        url="https://pub.example.org/paper",
        anchors=((anchor, "PDF"),),
        request_map={anchor: b"%PDF-1.4\ntiny"},  # correct magic but under 10KB
    )
    _patch_connection(monkeypatch, page)
    result = _run(page, tmp_path)

    assert result.kind == "no_pdf"


def test_cookie_banner_dismissed_before_pdf(tmp_path, monkeypatch):
    anchor = "https://pub.example.org/pdf"
    page = FakePage(
        url="https://pub.example.org/paper",
        anchors=((anchor, "PDF"),),
        request_map={anchor: BIG_PDF},
        cookie_button="#onetrust-accept-btn-handler",
    )
    _patch_connection(monkeypatch, page)
    result = _run(page, tmp_path)

    assert result.kind == "downloaded"
    # the OneTrust id is tried and accepted; no later selectors attempted.
    assert page.click_calls == ["#onetrust-accept-btn-handler"]


def test_budget_exceeded_is_error_before_navigation(tmp_path, monkeypatch):
    page = FakePage(
        url="https://pub.example.org/paper",
        anchors=(("https://pub.example.org/pdf", "PDF"),),
        request_map={"https://pub.example.org/pdf": BIG_PDF},
    )
    _patch_connection(monkeypatch, page)
    result = _run(page, tmp_path, budget=-1.0)  # already past the deadline

    assert result.kind == "error"
    assert "budget" in result.notes
    assert page.goto_calls == []  # nothing navigated
    assert page.closed is True  # still cleaned up


def test_page_closed_even_when_run_raises(tmp_path, monkeypatch):
    """Critical: the page is closed and NO exception escapes even if the scripted
    run blows up unexpectedly (outer except path)."""
    page = FakePage(url="https://pub.example.org/paper")
    _patch_connection(monkeypatch, page)

    async def _boom(**kwargs):
        raise RuntimeError("kaboom")

    monkeypatch.setattr(sb, "_run_scripted", _boom)
    result = _run(page, tmp_path)

    assert result.kind == "error"
    assert "kaboom" in result.notes
    assert page.closed is True


def test_goto_failure_is_error_and_closes_page(tmp_path, monkeypatch):
    """A Playwright goto failure is classified error (never raised) and the page
    is still closed."""
    page = FakePage(
        url="https://pub.example.org/paper",
        raise_on_goto=RuntimeError("net::ERR_TIMED_OUT"),
    )
    _patch_connection(monkeypatch, page)
    result = _run(page, tmp_path)

    assert result.kind == "error"
    assert "ERR_TIMED_OUT" in result.notes
    assert page.closed is True
