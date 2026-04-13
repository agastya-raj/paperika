from __future__ import annotations

from urllib.parse import urlparse, urlunparse
import re

from .models import ParsedInput

DOI_RE = re.compile(r"\b(10\.\d{4,9}/[-._;()/:A-Z0-9]+)\b", re.IGNORECASE)
URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)

PUBLISHER_HINTS = {
    "acm.org": "acm",
    "dl.acm.org": "acm",
    "ieeexplore.ieee.org": "ieee",
    "springer.com": "springer",
    "link.springer.com": "springer",
    "nature.com": "nature",
    "arxiv.org": "arxiv",
    "sciencedirect.com": "elsevier",
    "openreview.net": "openreview",
}


def normalize_url(url: str) -> str:
    parsed = urlparse(url.strip())
    scheme = parsed.scheme.lower() or "https"
    netloc = parsed.netloc.lower()
    path = re.sub(r"/+", "/", parsed.path or "/")
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    return urlunparse((scheme, netloc, path, "", parsed.query, ""))


def detect_publisher(url: str | None) -> str | None:
    if not url:
        return None
    host = urlparse(url).netloc.lower()
    for pattern, publisher in PUBLISHER_HINTS.items():
        if host == pattern or host.endswith("." + pattern):
            return publisher
    return None


def is_probable_pdf_url(url: str | None) -> bool:
    if not url:
        return False
    lowered = url.lower()
    return lowered.endswith(".pdf") or "pdf" in lowered.split("?")[0].split("#")[0].split("/")[-1]


def is_probable_viewer_url(url: str | None) -> bool:
    if not url:
        return False
    lowered = url.lower()
    return any(token in lowered for token in ["viewer", "pdfviewer", "epdf", "pdf/"])


def infer_input(raw_input: str) -> ParsedInput:
    text = raw_input.strip()
    doi_match = DOI_RE.search(text)
    url_match = URL_RE.search(text)
    doi = doi_match.group(1) if doi_match else None
    url = normalize_url(url_match.group(0)) if url_match else None
    title = text
    if doi:
        title = title.replace(doi_match.group(0), "").strip(" ,;:-")
    if url:
        title = title.replace(url_match.group(0), "").strip(" ,;:-")
    if not title:
        title = None
    return ParsedInput(
        raw_input=raw_input,
        title=title,
        doi=doi.lower() if doi else None,
        url=url,
        probable_pdf=is_probable_pdf_url(url),
        probable_viewer=is_probable_viewer_url(url),
        publisher_hint=detect_publisher(url),
    )
