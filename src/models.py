"""
Unified data schema for crawled page metadata.
Single source of truth used by crawler, extractor, NLP pipeline, and API.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import structlog

log = structlog.get_logger(__name__)


@dataclass
class SEOInsights:
    title_length: int = 0
    meta_desc_length: int = 0
    h1_count: int = 0
    has_canonical: bool = False
    has_og_tags: bool = False
    has_schema_markup: bool = False
    internal_link_count: int = 0
    external_link_count: int = 0
    word_count: int = 0
    keyword_density: dict[str, float] = field(default_factory=dict)
    title_too_long: bool = False       # >60 chars
    meta_desc_too_long: bool = False   # >160 chars
    thin_content: bool = False         # <300 words


@dataclass
class PageMetadata:
    # Identity
    url: str
    url_hash: str = ""
    domain: str = ""
    crawled_at: datetime = field(default_factory=datetime.utcnow)

    # Core HTML metadata
    title: str = ""
    meta_description: str = ""
    meta_keywords: list[str] = field(default_factory=list)
    canonical_url: str = ""
    lang: str = ""

    # Heading structure
    h1_tags: list[str] = field(default_factory=list)
    h2_tags: list[str] = field(default_factory=list)
    h3_tags: list[str] = field(default_factory=list)

    # Open Graph
    og_title: str = ""
    og_description: str = ""
    og_image: str = ""

    # Clean body content (boilerplate removed via trafilatura)
    body_text: str = ""
    content_hash: str = ""   # SHA-256(body_text) for change detection

    # NLP outputs
    topics: list[str] = field(default_factory=list)
    page_type: str = ""
    tfidf_keywords: list[tuple[str, float]] = field(default_factory=list)

    # SEO analysis
    seo: SEOInsights = field(default_factory=SEOInsights)

    # Crawl diagnostics
    status_code: int = 0
    crawl_duration_ms: int = 0
    error: Optional[str] = None

    # In production: raw HTML lives in S3/GCS; this stores the path
    raw_html_storage_path: str = ""

    def __post_init__(self):
        if self.url and not self.url_hash:
            self.url_hash = hashlib.sha256(self.url.encode()).hexdigest()
            log.debug("url_hash_computed", url=self.url, url_hash=self.url_hash[:12])

    def to_dict(self) -> dict:
        import dataclasses
        d = dataclasses.asdict(self)
        d["crawled_at"] = self.crawled_at.isoformat()
        return d
