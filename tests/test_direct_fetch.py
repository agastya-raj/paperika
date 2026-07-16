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


# ------------------------------------- Optica 202 JS bot challenge (AGA-489) ---
#
# The 202 on opg.optica.org's media endpoint is an Imperva/Distil JavaScript
# BOT CHALLENGE, not a "PDF is generating" placeholder: no Retry-After, no
# Location — the protocol lives in the body, which fetches a per-response
# checkjs URL (its response sets the cookie) and only then replays the media URL
# with r=1. The gate is on an ACTION, so re-GETting the same URL clears nothing
# at any retry count or delay. Skipping the checkjs hop lands on a CAPTCHA.


def _challenge_body(uri: str, token: str) -> bytes:
    """Optica's 202 stub: both instruction sites (the <script> fetch/replace and the
    <noscript> refresh) and a per-response ``s=`` token. Ampersands are written
    entity-escaped — the harder variant, exercising the unescape path; the captured
    wire body has them bare inside <script> (raw text ⇒ no entity decoding), which
    the same parser handles."""
    replay = f"/viewmedia.cfm?r=1&amp;uri={uri}&amp;seq=0"
    return (
        f'<noscript><meta http-equiv="Refresh" content="0;URL={replay}"></noscript>'
        f"<p>Please wait...</p>"
        f"<script>fetch('/checkjs.cfm?s={token}&amp;x=1')"
        f".then(() => window.location.replace('{replay}'))</script>"
    ).encode()


def test_js_challenge_urls_parsed_from_the_captured_wire_body():
    """Unit, pinned to the EXACT 553-byte body captured off opg.optica.org (bare
    ampersands: <script> content is raw text, so a real server does not escape it).
    Both URLs are resolved against the URL that served the challenge."""
    base = "https://opg.optica.org/viewmedia.cfm?uri=jocn-15-8-C242&html=true"
    wire = (
        '<noscript><meta http-equiv="Refresh" '
        'content="0;URL=/viewmedia.cfm?r=1&uri=jocn-15-8-C242&seq=0"></noscript>\n'
        "<p>Please wait...</p>\n"
        "<script>fetch('/checkjs.cfm?s=ABC123xyz&x=1')"
        ".then(() => window.location.replace('/viewmedia.cfm?r=1&uri=jocn-15-8-C242&seq=0'))</script>"
    )
    assert direct_fetch._js_challenge_urls(wire, base) == (
        "https://opg.optica.org/checkjs.cfm?s=ABC123xyz&x=1",
        "https://opg.optica.org/viewmedia.cfm?r=1&uri=jocn-15-8-C242&seq=0",
    )


def test_js_challenge_urls_fall_back_to_noscript_target():
    """The replay URL is taken from location.replace when present, else the
    <noscript> meta refresh (the server writes it in both places)."""
    base = "https://opg.optica.org/viewmedia.cfm?uri=x&html=true"
    noscript_only = '<noscript><meta http-equiv="Refresh" content="0;URL=/viewmedia.cfm?r=1&amp;uri=x"></noscript>'
    check, replay = direct_fetch._js_challenge_urls(noscript_only, base)
    assert replay == "https://opg.optica.org/viewmedia.cfm?r=1&uri=x"
    assert check is None  # no checkjs call in the body ⇒ nothing to hand the gate


def test_optica_js_challenge_handshake_downloads_pdf(tmp_path, monkeypatch):
    """Happy path, end to end on the REAL shapes: doi.org resolves onto the MEDIA
    endpoint (viewmedia.cfm?uri=…&html=true — no fixture modelled this before),
    which answers 202 + challenge; tier 1 must GET the body's checkjs URL on the
    SAME session, replay the r=1 URL (carrying the cookie checkjs set), follow the
    302 to the view_article interstitial and land the directpdfaccess PDF."""
    uri = "jocn-15-8-C242"
    hits: list[str] = []
    cookies: dict[str, str] = {}

    def handler(request):
        path, query = request.url.path, request.url.query.decode()
        hits.append(f"{path}?{query}")
        cookies[f"{path}?{query}"] = request.headers.get("cookie", "")
        # the gate is the Imperva cookie, exactly as on the wire: an uncookied hit
        # on the media endpoint is challenged, a cookied one is served.
        cookied = "incap_ses" in request.headers.get("cookie", "")
        if request.url.host == "doi.org":
            return httpx.Response(
                302, headers={"location": f"https://opg.optica.org/viewmedia.cfm?uri={uri}&html=true"}
            )
        if path == "/viewmedia.cfm" and not cookied:
            return httpx.Response(
                202, headers={"content-type": "text/html"}, content=_challenge_body(uri, "TOKEN-A")
            )
        if path == "/checkjs.cfm":
            # the response is what mints the Imperva cookie
            return httpx.Response(200, headers={"set-cookie": "incap_ses_1=cleared; Path=/"}, content=b"ok")
        if path == "/viewmedia.cfm":  # cookied ⇒ delivered
            return httpx.Response(
                302, headers={"location": f"https://opg.optica.org/view_article.cfm?pdfKey=KEY_9&uri={uri}"}
            )
        if path == "/view_article.cfm":
            return httpx.Response(
                200,
                headers={"content-type": "text/html"},
                content=(
                    b"<html><body>Your PDF will open shortly."
                    b'<a href="/directpdfaccess/KEY_9/' + uri.encode() + b'.pdf?da=1&seq=0">here</a>'
                    b"</body></html>"
                ),
            )
        if path.startswith("/directpdfaccess/"):
            return httpx.Response(200, headers={"content-type": "application/pdf"}, content=PDF_OK)
        return httpx.Response(404, content=b"nope")

    result = _fetch(tmp_path, monkeypatch, handler, doi="10.1364/jocn.487000")
    assert result.kind == "downloaded"
    assert Path(result.file_path).read_bytes() == PDF_OK
    # the handshake ran in the prescribed order: challenge -> checkjs -> r=1 replay
    checkjs = [h for h in hits if h.startswith("/checkjs.cfm")]
    assert checkjs == ["/checkjs.cfm?s=TOKEN-A&x=1"]  # token scraped, &amp; unescaped
    assert hits.index(checkjs[0]) < hits.index(f"/viewmedia.cfm?r=1&uri={uri}&seq=0")
    # the cookie checkjs set is carried on the replay ⇒ one shared session/jar
    assert "incap_ses_1=cleared" in cookies[f"/viewmedia.cfm?r=1&uri={uri}&seq=0"]


def test_js_challenge_token_is_scraped_per_response(tmp_path, monkeypatch):
    """The ``s=`` token is minted per response: a second challenge must be answered
    with the token from ITS OWN body, never a cached/hardcoded one."""
    uri = "oe-25-16-18553"
    tokens = iter(["TOKEN-1", "TOKEN-2"])
    seen: list[str] = []

    def handler(request):
        path, query = request.url.path, request.url.query.decode()
        if request.url.host == "doi.org":
            return httpx.Response(302, headers={"location": f"https://opg.optica.org/oe/fulltext.cfm?uri={uri}"})
        if path.endswith("/fulltext.cfm"):
            return httpx.Response(200, headers={"content-type": "text/html"}, content=b"<html>optica fulltext</html>")
        if path == "/checkjs.cfm":
            seen.append(query)
            return httpx.Response(200, content=b"ok")
        if path.endswith("/viewmedia.cfm"):
            # challenge, then RE-challenge the replay with a FRESH token, then serve
            token = next(tokens, None)
            if token is not None:
                return httpx.Response(
                    202, headers={"content-type": "text/html"}, content=_challenge_body(uri, token)
                )
            return httpx.Response(200, headers={"content-type": "application/pdf"}, content=PDF_OK)
        return httpx.Response(404, content=b"nope")

    result = _fetch(tmp_path, monkeypatch, handler, doi="10.1364/oe.25.018553")
    assert result.kind == "downloaded"
    assert seen == ["s=TOKEN-1&x=1", "s=TOKEN-2&x=1"]  # each answered from its own body


def test_js_challenge_rounds_are_bounded_and_miss_is_non_fatal(tmp_path, monkeypatch):
    """A gate that never clears is abandoned as a benign no_pdf (bounded rounds, no
    spin), leaving the ladder free to escalate."""
    uri = "oe-25-16-18553"
    checkjs_hits = {"n": 0}

    def handler(request):
        path = request.url.path
        if request.url.host == "doi.org":
            return httpx.Response(302, headers={"location": f"https://opg.optica.org/viewmedia.cfm?uri={uri}&html=true"})
        if path == "/checkjs.cfm":
            checkjs_hits["n"] += 1
            return httpx.Response(200, content=b"ok")
        if path == "/viewmedia.cfm":
            return httpx.Response(
                202, headers={"content-type": "text/html"}, content=_challenge_body(uri, "TOKEN-X")
            )
        return httpx.Response(404, content=b"nope")

    result = _fetch(tmp_path, monkeypatch, handler, doi="10.1364/oe.25.018553")
    assert result.kind == "no_pdf"
    # the handshake is retried a bounded number of rounds, then abandoned — no spin
    assert checkjs_hits["n"] == direct_fetch._JS_CHALLENGE_MAX_ROUNDS


def test_unrecognized_challenge_stub_falls_through_with_body_traced(tmp_path, monkeypatch):
    """Imperva changes shape: a 202 whose body carries no checkjs/replay we can read
    must NOT be fatal — it resolves to an ordinary no_pdf, and the stub body is
    carried into the notes so the change is diagnosable instead of silent."""
    stub = b"<html><body>\n\n   Please wait...   \n<p>nothing we can parse here</p></body></html>"

    def handler(request):
        if request.url.host == "doi.org":
            return httpx.Response(302, headers={"location": "https://opg.optica.org/viewmedia.cfm?uri=oe-1-1-1&html=true"})
        if request.url.path == "/viewmedia.cfm":
            return httpx.Response(202, headers={"content-type": "text/html"}, content=stub)
        return httpx.Response(404, content=b"nope")

    result = _fetch(tmp_path, monkeypatch, handler, doi="10.1364/oe.1.000001")
    assert result.kind == "no_pdf"
    assert "unresolved 202 challenge stub" in result.notes
    assert "nothing we can parse here" in result.notes  # truncated + whitespace-collapsed


def test_js_challenge_never_takes_the_noscript_shortcut(tmp_path, monkeypatch):
    """The checkjs hop is load-bearing: replaying the <noscript> target WITHOUT it
    lands on a CAPTCHA (proven live). So a body offering only the noscript refresh
    and no checkjs call must NOT be replayed — we miss and escalate instead."""
    replayed: list[str] = []
    noscript_only = (
        b'<noscript><meta http-equiv="Refresh" '
        b'content="0;URL=/viewmedia.cfm?r=1&amp;uri=oe-1-1-1&amp;seq=0"></noscript><p>Please wait...</p>'
    )

    def handler(request):
        query = request.url.query.decode()
        if request.url.host == "doi.org":
            return httpx.Response(302, headers={"location": "https://opg.optica.org/viewmedia.cfm?uri=oe-1-1-1&html=true"})
        if request.url.path == "/viewmedia.cfm" and "r=1" in query:
            replayed.append(query)  # would be the CAPTCHA in production
            return httpx.Response(200, headers={"content-type": "application/pdf"}, content=PDF_OK)
        if request.url.path == "/viewmedia.cfm":
            return httpx.Response(202, headers={"content-type": "text/html"}, content=noscript_only)
        return httpx.Response(404, content=b"nope")

    result = _fetch(tmp_path, monkeypatch, handler, doi="10.1364/oe.1.000001")
    assert result.kind == "no_pdf"
    assert replayed == []  # the r=1 shortcut was never taken
    assert "unresolved 202 challenge stub" in result.notes


@pytest.mark.parametrize(
    "internal_url, marker",
    [
        ("http://127.0.0.1:9224/checkjs.cfm?s=T&x=1", "127.0.0.1"),   # the CDP control port
        ("http://169.254.169.254/checkjs.cfm?s=T&x=1", "169.254"),    # cloud metadata
    ],
)
def test_js_challenge_check_url_to_internal_host_is_refused(tmp_path, monkeypatch, internal_url, marker):
    """The handshake URLs come out of a publisher-controlled body ⇒ DATA. A stub
    pointing the authenticated GET at an internal target must be refused by the same
    SSRF gate every other hop passes: the host is never contacted, the tier reports
    a benign no_pdf."""
    requested: list[str] = []
    body = (
        f"<p>Please wait...</p><script>fetch('{internal_url}')"
        f".then(() => window.location.replace('/viewmedia.cfm?r=1&amp;uri=oe-1-1-1&amp;seq=0'))</script>"
    ).encode()

    def handler(request):
        requested.append(str(request.url))
        if request.url.host == "doi.org":
            return httpx.Response(302, headers={"location": "https://opg.optica.org/viewmedia.cfm?uri=oe-1-1-1&html=true"})
        if request.url.path == "/viewmedia.cfm" and "r=1" not in request.url.query.decode():
            return httpx.Response(202, headers={"content-type": "text/html"}, content=body)
        # only reachable if the guard failed: would serve a fake PDF
        return httpx.Response(200, headers={"content-type": "application/pdf"}, content=PDF_OK)

    result = _fetch(tmp_path, monkeypatch, handler, doi="10.1364/oe.1.000001")
    assert result.kind == "no_pdf"
    assert all(marker not in u for u in requested)  # internal target never contacted


def test_js_challenge_replay_url_to_internal_host_is_refused(tmp_path, monkeypatch):
    """Same gate on the OTHER scraped URL: a challenge whose replay target is
    internal is refused after the (public) checkjs hop."""
    requested: list[str] = []
    body = (
        b"<p>Please wait...</p><script>fetch('/checkjs.cfm?s=T&amp;x=1')"
        b".then(() => window.location.replace('http://10.0.0.5/viewmedia.cfm?r=1'))</script>"
    )

    def handler(request):
        requested.append(str(request.url))
        if request.url.host == "doi.org":
            return httpx.Response(302, headers={"location": "https://opg.optica.org/viewmedia.cfm?uri=oe-1-1-1&html=true"})
        if request.url.path == "/checkjs.cfm":
            return httpx.Response(200, content=b"ok")
        if request.url.path == "/viewmedia.cfm":
            return httpx.Response(202, headers={"content-type": "text/html"}, content=body)
        return httpx.Response(200, headers={"content-type": "application/pdf"}, content=PDF_OK)

    result = _fetch(tmp_path, monkeypatch, handler, doi="10.1364/oe.1.000001")
    assert result.kind == "no_pdf"
    assert all("10.0.0.5" not in u for u in requested)


def test_optica_html_interstitial_followed_to_directpdfaccess(tmp_path, monkeypatch):
    # JOCN's viewmedia.cfm returns 200 with a "your PDF will open shortly"
    # interstitial whose only link is a relative /directpdfaccess/…/file.pdf.
    # Tier 1 must follow that ONE hop to the PDF (live gap: fell to codex, 187s).
    interstitial = (
        b"<html><body>Your PDF will open shortly."
        b'<a href="/directpdfaccess/KEY_408227/jocn-11-5-226.pdf?da=1&id=408227&seq=0">here</a>'
        b"</body></html>"
    )

    def handler(request):
        host, path = request.url.host, request.url.path
        if host == "doi.org":
            return httpx.Response(302, headers={"location": "https://opg.optica.org/jocn/fulltext.cfm?uri=jocn-11-5-226"})
        if path.endswith("/fulltext.cfm"):
            return httpx.Response(200, headers={"content-type": "text/html"}, content=b"<html>jocn abstract</html>")
        if path.endswith("/viewmedia.cfm"):
            return httpx.Response(200, headers={"content-type": "text/html"}, content=interstitial)
        if "/directpdfaccess/" in path:
            return httpx.Response(200, headers={"content-type": "application/pdf"}, content=PDF_OK)
        return httpx.Response(404, content=b"nope")

    result = _fetch(tmp_path, monkeypatch, handler, doi="10.1364/jocn.11.000226")
    assert result.kind == "downloaded"
    assert "interstitial pdf" in result.notes


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
