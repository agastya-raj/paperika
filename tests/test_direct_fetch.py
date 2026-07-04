"""AGA-339 tier-1 direct-fetch tests — zero live network, zero playwright.

httpx is mocked with ``httpx.MockTransport`` (installed via the module's
``_TEST_TRANSPORT`` seam) and ``export_chrome_cookies`` is monkeypatched to an
async stub, so nothing here spins up a browser or touches the wire.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import pytest

from paperika.bridge import direct_fetch

# A body that passes the %PDF- magic + 10 KB floor gate.
PDF_OK = b"%PDF-1.4\n" + b"0" * 20_000
# A %PDF- body that is too small (fails the 10 KB floor).
PDF_SMALL = b"%PDF-1.4\nshort"


def _install_transport(monkeypatch, handler) -> None:
    monkeypatch.setattr(direct_fetch, "_TEST_TRANSPORT", httpx.MockTransport(handler))


@pytest.fixture(autouse=True)
def _no_cookies(monkeypatch):
    """Default: cookie export returns [] fast (never launches playwright). Tests
    that care about cookies override this."""
    async def _empty(cdp_http_url: str):
        return []
    monkeypatch.setattr(direct_fetch, "export_chrome_cookies", _empty)


@pytest.fixture(autouse=True)
def _public_dns(monkeypatch):
    """The SSRF guard resolves every fetched host; map the test publisher
    hostnames to a public IP so the guard admits them without touching real DNS.
    IP-literal internal targets (127.0.0.1, 169.254...) are checked directly and
    never hit this resolver, so they stay blocked."""
    monkeypatch.setattr(direct_fetch, "_resolve_host", lambda host: ["93.184.216.34"])


def _fetch(tmp_path: Path, monkeypatch, handler, *, doi="10.1364/jocn.42", budget_seconds=20.0):
    _install_transport(monkeypatch, handler)
    return asyncio.run(
        direct_fetch.attempt_direct_fetch(
            doi=doi,
            title="Some Title",
            start_url=f"https://doi.org/{doi}",
            download_dir=tmp_path / "downloads",
            cdp_http_url="http://127.0.0.1:9224",
            budget_seconds=budget_seconds,
        )
    )


# ---------------------------------------------------- citation_pdf_url unit ---


def test_citation_pdf_url_name_first():
    html = '<meta name="citation_pdf_url" content="https://pub.example.org/a.pdf">'
    assert direct_fetch._citation_pdf_url(html, "https://pub.example.org/article") == (
        "https://pub.example.org/a.pdf"
    )


def test_citation_pdf_url_content_first():
    html = "<meta content='https://pub.example.org/b.pdf' name='citation_pdf_url'/>"
    assert direct_fetch._citation_pdf_url(html, "https://pub.example.org/article") == (
        "https://pub.example.org/b.pdf"
    )


def test_citation_pdf_url_relative_resolved_against_base():
    html = '<meta name="citation_pdf_url" content="/content/c.pdf">'
    assert direct_fetch._citation_pdf_url(html, "https://pub.example.org/journal/article") == (
        "https://pub.example.org/content/c.pdf"
    )


def test_citation_pdf_url_html_entities_unescaped():
    html = '<meta name="citation_pdf_url" content="https://pub.example.org/d.pdf?a=1&amp;b=2">'
    assert direct_fetch._citation_pdf_url(html, "https://pub.example.org/article") == (
        "https://pub.example.org/d.pdf?a=1&b=2"
    )


def test_citation_pdf_url_absent_returns_none():
    assert direct_fetch._citation_pdf_url("<html><body>no meta</body></html>", "https://x.org") is None


# ------------------------------------------------------ publisher rule unit ---


@pytest.mark.parametrize(
    "final_url, doi, expected",
    [
        (
            "https://ieeexplore.ieee.org/document/9424113",
            "10.1109/x",
            "https://ieeexplore.ieee.org/stampPDF/getPDF.jsp?tp=&arnumber=9424113",
        ),
        (
            "https://opg.optica.org/abstract.cfm?URI=jocn-17-9-D106",
            "10.1364/jocn.560987",
            "https://opg.optica.org/viewmedia.cfm?uri=jocn-17-9-D106&seq=0",
        ),
        (
            "https://www.osapublishing.org/abstract.cfm?uri=OFC-2025-M2C.5",
            "10.1364/ofc.2025.m2c.5",
            "https://www.osapublishing.org/viewmedia.cfm?uri=OFC-2025-M2C.5&seq=0",
        ),
        (
            "https://link.springer.com/article/10.1007/s11276-024-03700-w",
            "10.1007/s11276-024-03700-w",
            "https://link.springer.com/content/pdf/10.1007/s11276-024-03700-w.pdf",
        ),
        (
            "https://onlinelibrary.wiley.com/doi/10.1002/lpor.202400123",
            "10.1002/lpor.202400123",
            "https://onlinelibrary.wiley.com/doi/pdf/10.1002/lpor.202400123?download=true",
        ),
        (
            "https://www.mdpi.com/2076-3417/11/1/100",
            "10.3390/app11010100",
            "https://www.mdpi.com/2076-3417/11/1/100/pdf",
        ),
        (
            "https://www.nature.com/articles/s41586-024-01234-5",
            "10.1038/s41586-024-01234-5",
            "https://www.nature.com/articles/s41586-024-01234-5.pdf",
        ),
        (
            "https://arxiv.org/abs/2401.01234",
            "10.48550/arxiv.2401.01234",
            "https://arxiv.org/pdf/2401.01234",
        ),
    ],
)
def test_publisher_candidate_rules(final_url, doi, expected):
    assert direct_fetch._publisher_candidates(final_url, doi) == [expected]


def test_candidate_order_citation_before_publisher_rule():
    html = b'<meta name="citation_pdf_url" content="https://ieeexplore.ieee.org/priority.pdf">'
    cands = direct_fetch._candidate_urls("https://ieeexplore.ieee.org/document/9424113", "10.1109/x", html)
    assert cands[0] == "https://ieeexplore.ieee.org/priority.pdf"
    assert "https://ieeexplore.ieee.org/stampPDF/getPDF.jsp?tp=&arnumber=9424113" in cands


def test_unknown_publisher_yields_no_rule_candidate():
    assert direct_fetch._publisher_candidates("https://example.org/paper", "10.1/x") == []


def test_sanitize_doi():
    assert direct_fetch.sanitize_doi("10.1364/jocn.999999") == "10.1364_jocn.999999"
    assert direct_fetch.sanitize_doi("10.1145/3544548.3580875") == "10.1145_3544548.3580875"


# ------------------------------------------------------ end-to-end downloads ---


def test_citation_pdf_url_drives_download(tmp_path, monkeypatch):
    def handler(request):
        host, path = request.url.host, request.url.path
        if host == "doi.org":
            return httpx.Response(
                200,
                headers={"content-type": "text/html"},
                content=b'<html><head><meta name="citation_pdf_url" '
                b'content="https://pub.example.org/full.pdf"></head></html>',
            )
        if host == "pub.example.org" and path == "/full.pdf":
            return httpx.Response(200, headers={"content-type": "application/pdf"}, content=PDF_OK)
        return httpx.Response(404, content=b"nope")

    result = _fetch(tmp_path, monkeypatch, handler)
    assert result.kind == "downloaded"
    assert result.final_url == "https://pub.example.org/full.pdf"
    written = Path(result.file_path)
    assert written.exists() and written.read_bytes() == PDF_OK


def test_file_written_with_sanitized_name(tmp_path, monkeypatch):
    def handler(request):
        if request.url.host == "doi.org":
            return httpx.Response(200, headers={"content-type": "application/pdf"}, content=PDF_OK)
        return httpx.Response(404)

    result = _fetch(tmp_path, monkeypatch, handler, doi="10.1364/jocn.999999")
    assert result.kind == "downloaded"
    assert Path(result.file_path) == tmp_path / "downloads" / "10.1364_jocn.999999.pdf"


def test_landing_is_pdf_after_redirect(tmp_path, monkeypatch):
    def handler(request):
        if request.url.host == "doi.org":
            return httpx.Response(302, headers={"location": "https://pub.example.org/inline.pdf"})
        return httpx.Response(200, headers={"content-type": "application/pdf"}, content=PDF_OK)

    result = _fetch(tmp_path, monkeypatch, handler)
    assert result.kind == "downloaded"
    assert result.final_url == "https://pub.example.org/inline.pdf"
    assert Path(result.file_path).read_bytes() == PDF_OK


def test_ieee_publisher_rule_end_to_end(tmp_path, monkeypatch):
    def handler(request):
        host, path = request.url.host, request.url.path
        if host == "doi.org":
            return httpx.Response(302, headers={"location": "https://ieeexplore.ieee.org/document/9424113"})
        if host == "ieeexplore.ieee.org" and path == "/document/9424113":
            return httpx.Response(200, headers={"content-type": "text/html"}, content=b"<html>ieee article</html>")
        if host == "ieeexplore.ieee.org" and path == "/stampPDF/getPDF.jsp":
            return httpx.Response(200, headers={"content-type": "application/pdf"}, content=PDF_OK)
        return httpx.Response(404, content=b"nope")

    result = _fetch(tmp_path, monkeypatch, handler)
    assert result.kind == "downloaded"
    assert "stampPDF" in (result.final_url or "")
    assert Path(result.file_path).read_bytes() == PDF_OK


# ----------------------------------------------------------- %PDF+size gate ---


def test_small_pdf_body_rejected_as_no_pdf(tmp_path, monkeypatch):
    def handler(request):
        if request.url.host == "doi.org":
            return httpx.Response(
                200,
                headers={"content-type": "text/html"},
                content=b'<meta name="citation_pdf_url" content="https://pub.example.org/tiny.pdf">',
            )
        return httpx.Response(200, headers={"content-type": "application/pdf"}, content=PDF_SMALL)

    result = _fetch(tmp_path, monkeypatch, handler)
    assert result.kind == "no_pdf"
    assert not (tmp_path / "downloads").exists() or not list((tmp_path / "downloads").glob("*.pdf"))


def test_html_body_named_pdf_rejected_as_no_pdf(tmp_path, monkeypatch):
    def handler(request):
        if request.url.host == "doi.org":
            return httpx.Response(
                200,
                headers={"content-type": "text/html"},
                content=b'<meta name="citation_pdf_url" content="https://pub.example.org/login.pdf">',
            )
        # candidate URL returns an HTML login page, not a PDF
        return httpx.Response(200, headers={"content-type": "text/html"}, content=b"<html>sign in</html>")

    result = _fetch(tmp_path, monkeypatch, handler)
    assert result.kind == "no_pdf"


def test_landing_ok_no_candidate_is_no_pdf(tmp_path, monkeypatch):
    def handler(request):
        # reachable landing, unknown publisher, no citation meta ⇒ no candidates
        return httpx.Response(200, headers={"content-type": "text/html"}, content=b"<html>plain</html>")

    result = _fetch(tmp_path, monkeypatch, handler, doi="10.9999/unknown")
    assert result.kind == "no_pdf"


# --------------------------------------------------------- wall classification ---


def test_403_landing_is_wall(tmp_path, monkeypatch):
    def handler(request):
        return httpx.Response(403, content=b"forbidden")

    result = _fetch(tmp_path, monkeypatch, handler)
    assert result.kind == "wall"


def test_cloudflare_challenge_body_is_wall(tmp_path, monkeypatch):
    def handler(request):
        return httpx.Response(
            503,
            headers={"content-type": "text/html"},
            content=b"<html><title>Just a moment...</title><div class='cf-chl'></div></html>",
        )

    result = _fetch(tmp_path, monkeypatch, handler)
    assert result.kind == "wall"


def test_candidate_403_after_ok_landing_is_wall(tmp_path, monkeypatch):
    def handler(request):
        if request.url.host == "doi.org":
            return httpx.Response(
                200,
                headers={"content-type": "text/html"},
                content=b'<meta name="citation_pdf_url" content="https://pub.example.org/walled.pdf">',
            )
        return httpx.Response(403, content=b"forbidden")

    result = _fetch(tmp_path, monkeypatch, handler)
    assert result.kind == "wall"


# ----------------------------------------------------------- error / budget ---


def test_network_error_on_landing_is_error(tmp_path, monkeypatch):
    def handler(request):
        raise httpx.ConnectError("connection refused")

    result = _fetch(tmp_path, monkeypatch, handler)
    assert result.kind == "error"


def test_budget_respected_slow_handler_returns_error(tmp_path, monkeypatch):
    async def handler(request):
        await asyncio.sleep(5)
        return httpx.Response(200, content=PDF_OK)

    # Tight budget + a 5s handler ⇒ the overall asyncio.timeout fires ⇒ error, no hang.
    result = _fetch(tmp_path, monkeypatch, handler, budget_seconds=0.1)
    assert result.kind == "error"


# --------------------------------------------------------------- cookies ---


def test_cookie_export_failure_proceeds_cookieless(tmp_path, monkeypatch):
    async def _boom(cdp_http_url: str):
        raise RuntimeError("cdp unreachable")
    monkeypatch.setattr(direct_fetch, "export_chrome_cookies", _boom)

    def handler(request):
        if request.url.host == "doi.org":
            return httpx.Response(200, headers={"content-type": "application/pdf"}, content=PDF_OK)
        return httpx.Response(404)

    result = _fetch(tmp_path, monkeypatch, handler)
    assert result.kind == "downloaded"
    assert Path(result.file_path).read_bytes() == PDF_OK


def test_exported_cookies_are_sent_domain_aware(tmp_path, monkeypatch):
    async def _cookies(cdp_http_url: str):
        return [{"name": "SESSION", "value": "tok123", "domain": "pub.example.org", "path": "/"}]
    monkeypatch.setattr(direct_fetch, "export_chrome_cookies", _cookies)

    seen: dict[str, str] = {}

    def handler(request):
        host = request.url.host
        seen[host] = request.headers.get("cookie", "")
        if host == "doi.org":
            return httpx.Response(
                200,
                headers={"content-type": "text/html"},
                content=b'<meta name="citation_pdf_url" content="https://pub.example.org/full.pdf">',
            )
        return httpx.Response(200, headers={"content-type": "application/pdf"}, content=PDF_OK)

    result = _fetch(tmp_path, monkeypatch, handler)
    assert result.kind == "downloaded"
    # cookie scoped to pub.example.org is sent there, not to doi.org
    assert "SESSION=tok123" in seen["pub.example.org"]
    assert "SESSION" not in seen.get("doi.org", "")


def test_build_cookie_jar_skips_malformed_entries():
    jar = direct_fetch._build_cookie_jar(
        [
            {"name": "good", "value": "1", "domain": "x.org", "path": "/"},
            {"value": "no-name", "domain": "x.org"},
            {"name": "no-value", "domain": "x.org"},
            "not-a-dict",  # type: ignore[list-item]
        ]
    )
    names = {c.name for c in jar.jar}
    assert names == {"good"}


def test_export_chrome_cookies_degrades_to_empty(monkeypatch):
    import playwright.async_api as pw_async

    def _raise():
        raise RuntimeError("no driver")

    monkeypatch.setattr(pw_async, "async_playwright", _raise)
    assert asyncio.run(direct_fetch.export_chrome_cookies("http://127.0.0.1:9224")) == []


# ------------------------------------------------------- SSRF guard (finding 1) ---


def test_is_public_http_url_classifies_targets(monkeypatch):
    """Unit: http(s)+public-host only. IP literals to internal ranges, non-http(s)
    schemes, localhost, and a hostname that RESOLVES to loopback are all refused."""
    resolves = {
        "opg.optica.org": ["93.184.216.34"],       # public ⇒ allowed
        "rebind.evil.example": ["127.0.0.1"],        # DNS-rebind to loopback ⇒ blocked
    }
    monkeypatch.setattr(direct_fetch, "_resolve_host", lambda host: resolves[host])

    assert direct_fetch.is_public_http_url("https://opg.optica.org/x.pdf") is True
    # internal IP literals (never resolved — checked directly)
    assert direct_fetch.is_public_http_url("http://127.0.0.1:9224/json/version") is False
    assert direct_fetch.is_public_http_url("http://169.254.169.254/latest/meta-data/") is False
    assert direct_fetch.is_public_http_url("http://10.0.0.5/x") is False
    assert direct_fetch.is_public_http_url("http://[::1]:9224/x") is False
    # non-http(s) scheme and localhost are refused without a lookup
    assert direct_fetch.is_public_http_url("file:///etc/hosts") is False
    assert direct_fetch.is_public_http_url("http://localhost:9224/x") is False
    # a name that resolves to an internal address is refused (DNS rebinding)
    assert direct_fetch.is_public_http_url("http://rebind.evil.example/x") is False


def test_direct_fetch_refuses_redirect_to_internal_host(tmp_path, monkeypatch):
    """Finding 1 (HIGH): a hostile publisher 302 that points the authenticated GET
    at the CDP control port must be refused — the internal host is NEVER contacted
    and the attempt classifies as a benign error, no bytes landed."""
    requested: list[str] = []

    def handler(request):
        requested.append(str(request.url))
        if request.url.host == "pub.example.org":
            return httpx.Response(302, headers={"location": "http://127.0.0.1:9224/json/version?x=pdf"})
        # only reachable if the guard failed: would serve a fake PDF
        return httpx.Response(200, content=PDF_OK)

    _install_transport(monkeypatch, handler)
    result = asyncio.run(
        direct_fetch.attempt_direct_fetch(
            doi="10.1364/jocn.42", title="t", start_url="https://pub.example.org/article",
            download_dir=tmp_path / "downloads", cdp_http_url="http://127.0.0.1:9224",
        )
    )
    assert result.kind == "error"
    assert all("127.0.0.1" not in u for u in requested)  # CDP port never contacted
    assert not (tmp_path / "downloads").exists() or not list((tmp_path / "downloads").glob("*.pdf"))


def test_direct_fetch_refuses_citation_meta_to_internal_host(tmp_path, monkeypatch):
    """Finding 1 (HIGH): an attacker-controlled citation_pdf_url meta aimed at the
    cloud-metadata endpoint is never fetched; the tier reports a benign no_pdf."""
    requested: list[str] = []
    landing = (
        b'<html><head><meta name="citation_pdf_url" '
        b'content="http://169.254.169.254/latest/meta-data/x.pdf"></head></html>'
    )

    def handler(request):
        requested.append(str(request.url))
        if request.url.host == "pub.example.org":
            return httpx.Response(200, headers={"content-type": "text/html"}, content=landing)
        return httpx.Response(200, content=PDF_OK)

    _install_transport(monkeypatch, handler)
    result = asyncio.run(
        direct_fetch.attempt_direct_fetch(
            doi="10.9999/unknown", title="t", start_url="https://pub.example.org/article",
            download_dir=tmp_path / "downloads", cdp_http_url="http://127.0.0.1:9224",
        )
    )
    assert result.kind == "no_pdf"
    assert all("169.254" not in u for u in requested)  # metadata endpoint never contacted
