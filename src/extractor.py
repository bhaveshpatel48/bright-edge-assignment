"""
Content extraction pipeline.

Two-stage approach:
1. trafilatura  — removes nav/footer/ads/boilerplate, returns main content text
2. BeautifulSoup — extracts structured HTML signals (title, meta, headings, links, OG tags)

trafilatura uses a content-density algorithm (similar to Mozilla Readability)
that identifies the main article block by text-to-HTML-tag ratio, giving ~90%
accuracy vs. naive get_text() which includes cookie banners and navigation.
"""
from __future__ import annotations

import hashlib
import re
from urllib.parse import urlparse

import structlog
import trafilatura
from bs4 import BeautifulSoup

from models import PageMetadata, SEOInsights

log = structlog.get_logger(__name__)


def extract(url: str, html: str, status_code: int, duration_ms: int) -> PageMetadata:
    """Full extraction pass — returns a populated PageMetadata."""
    page = PageMetadata(url=url, status_code=status_code, crawl_duration_ms=duration_ms)
    page.domain = urlparse(url).netloc

    if not html:
        page.error = "empty response"
        log.warning("empty_response", url=url)
        return page

    soup = BeautifulSoup(html, "lxml")
    _extract_meta(soup, page)
    _extract_body(html, page)
    _extract_links(soup, url, page)
    _compute_seo_flags(page)

    log.info(
        "extracted",
        url=url,
        title=page.title[:60],
        page_type=page.page_type,
        word_count=page.seo.word_count,
        h1_count=page.seo.h1_count,
    )
    return page


def _extract_meta(soup: BeautifulSoup, page: PageMetadata) -> None:
    """Extract title, meta tags, headings, Open Graph, and JSON-LD."""
    title_tag = soup.find("title")
    page.title = title_tag.get_text(strip=True) if title_tag else ""

    for meta in soup.find_all("meta"):
        name = (meta.get("name") or meta.get("property") or "").lower()
        content = meta.get("content", "")
        if name == "description":
            page.meta_description = content
        elif name == "keywords":
            page.meta_keywords = [k.strip() for k in content.split(",") if k.strip()]
        elif name == "og:title":
            page.og_title = content
        elif name == "og:description":
            page.og_description = content
        elif name == "og:image":
            page.og_image = content

    page.seo.has_og_tags = bool(page.og_title or page.og_description)

    canonical = soup.find("link", rel="canonical")
    page.canonical_url = canonical.get("href", "") if canonical else ""
    page.seo.has_canonical = bool(page.canonical_url)

    html_tag = soup.find("html")
    page.lang = html_tag.get("lang", "") if html_tag else ""

    page.h1_tags = [h.get_text(strip=True) for h in soup.find_all("h1")]
    page.h2_tags = [h.get_text(strip=True) for h in soup.find_all("h2")]
    page.h3_tags = [h.get_text(strip=True) for h in soup.find_all("h3")]
    page.seo.h1_count = len(page.h1_tags)

    page.seo.has_schema_markup = bool(soup.find("script", {"type": "application/ld+json"}))


def _extract_body(html: str, page: PageMetadata) -> None:
    """
    Use trafilatura to strip boilerplate and extract main content.
    Falls back to raw BeautifulSoup text for JS-rendered SPAs.
    """
    clean_text = trafilatura.extract(html, include_comments=False, include_tables=True, no_fallback=False)

    if not clean_text:
        soup = BeautifulSoup(html, "lxml")
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
        clean_text = re.sub(r"\s+", " ", soup.get_text(separator=" ", strip=True)).strip()
        log.debug("trafilatura_fallback", url=page.url)

    page.body_text = clean_text or ""
    page.seo.word_count = len(page.body_text.split()) if page.body_text else 0
    page.content_hash = hashlib.sha256(page.body_text.encode()).hexdigest()


def _extract_links(soup: BeautifulSoup, base_url: str, page: PageMetadata) -> None:
    """Count internal vs external links."""
    base_domain = urlparse(base_url).netloc
    internal, external = 0, 0
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith(("#", "mailto:")):
            continue
        parsed = urlparse(href)
        if not parsed.netloc or parsed.netloc == base_domain:
            internal += 1
        else:
            external += 1
    page.seo.internal_link_count = internal
    page.seo.external_link_count = external


def _compute_seo_flags(page: PageMetadata) -> None:
    """Derive advisory SEO flags."""
    seo = page.seo
    seo.title_length = len(page.title)
    seo.meta_desc_length = len(page.meta_description)
    seo.title_too_long = seo.title_length > 60
    seo.meta_desc_too_long = seo.meta_desc_length > 160
    seo.thin_content = 0 < seo.word_count < 300
