from paperika.normalize import infer_input, normalize_url, detect_publisher


def test_infer_input_extracts_doi_url_and_flags():
    parsed = infer_input("Attention Is All You Need 10.48550/arXiv.1706.03762 https://arxiv.org/pdf/1706.03762.pdf")
    assert parsed.doi == "10.48550/arxiv.1706.03762"
    assert parsed.url == "https://arxiv.org/pdf/1706.03762.pdf"
    assert parsed.probable_pdf is True
    assert parsed.publisher_hint == "arxiv"
    assert "Attention Is All You Need" in (parsed.title or "")


def test_normalize_url_and_publisher():
    url = normalize_url("HTTPS://DL.ACM.ORG/doi/pdf/10.1145/12345/")
    assert url == "https://dl.acm.org/doi/pdf/10.1145/12345"
    assert detect_publisher(url) == "acm"
