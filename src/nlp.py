"""
NLP pipeline for topic classification and keyword extraction.

Two-layer approach:
1. KeyBERT (sentence-transformer embeddings) — extracts semantically central keyphrases.
   No labeled training data required. Generalizes across any domain.
2. TF-IDF (sklearn) — fast statistical keyword scoring. Used as a fallback when
   KeyBERT is unavailable and for computing per-keyword density.

Page type classification uses URL path signals + body signals (rule-based).
This is intentionally fast — zero network calls, runs in-process.
For production: swap KeyBERT for a GPU-accelerated batch inference service.
"""
from __future__ import annotations

import re
from urllib.parse import urlparse

import structlog
from sklearn.feature_extraction.text import TfidfVectorizer

from models import PageMetadata

log = structlog.get_logger(__name__)

_keybert_model = None

_PAGE_TYPE_SIGNALS = {
    "product": {
        "url": ["dp/", "/p/", "/product", "/item", "/sku"],
        "body": ["add to cart", "buy now", "price", "in stock", "review", "rating"],
    },
    "article": {
        "url": ["/blog/", "/news/", "/article/", "/post/", "/story/"],
        "body": ["published", "author", "min read", "share", "comment"],
    },
    "category": {
        "url": ["/category/", "/cat/", "/c/", "/department/", "/collection/"],
        "body": ["filter", "sort by", "showing", "results", "refine"],
    },
    "homepage": {
        "url": [],
        "body": ["welcome", "explore", "featured", "shop now", "learn more"],
    },
}


def _get_keybert():
    global _keybert_model
    if _keybert_model is None:
        try:
            from keybert import KeyBERT
            _keybert_model = KeyBERT(model="all-MiniLM-L6-v2")
            log.info("keybert_loaded", model="all-MiniLM-L6-v2")
        except ImportError:
            log.warning("keybert_unavailable", fallback="tfidf")
    return _keybert_model


def enrich(page: PageMetadata) -> PageMetadata:
    """
    Run full NLP enrichment on an extracted PageMetadata.
    Mutates and returns the same object.
    """
    if not page.body_text:
        return page

    page.page_type = _classify_page_type(page.url, page.body_text)
    page.topics = _extract_topics(page.body_text, page.title)
    page.tfidf_keywords = _tfidf_keywords(page.body_text)
    page.seo.keyword_density = _keyword_density(page.body_text, page.topics)

    log.info(
        "enriched",
        url=page.url,
        page_type=page.page_type,
        topic_count=len(page.topics),
        top_topics=page.topics[:3],
    )
    return page


def _extract_topics(body: str, title: str = "") -> list[str]:
    """
    Primary: KeyBERT keyphrases via MMR (reduces redundancy).
    Fallback: TF-IDF top-10 unigrams.
    """
    text = f"{title}. {body}" if title else body
    text = text[:2000]

    model = _get_keybert()
    if model:
        try:
            results = model.extract_keywords(
                text,
                keyphrase_ngram_range=(1, 2),
                stop_words="english",
                use_mmr=True,
                diversity=0.5,
                top_n=10,
            )
            return [kw for kw, _ in results]
        except Exception as exc:
            log.warning("keybert_failed", error=str(exc), fallback="tfidf")

    return [kw for kw, _ in _tfidf_keywords(text, top_n=10)]


def _tfidf_keywords(text: str, top_n: int = 20) -> list[tuple[str, float]]:
    """Single-document TF-IDF keyword scoring."""
    if not text or len(text.split()) < 5:
        return []
    try:
        vectorizer = TfidfVectorizer(max_features=top_n, stop_words="english", ngram_range=(1, 2), min_df=1)
        matrix = vectorizer.fit_transform([text])
        scores = zip(vectorizer.get_feature_names_out(), matrix.toarray()[0])
        return sorted(scores, key=lambda x: x[1], reverse=True)[:top_n]
    except Exception:
        return []


def _keyword_density(body: str, keywords: list[str]) -> dict[str, float]:
    """Keyword density = occurrences / total_words × 100. >3% = stuffing risk."""
    if not body or not keywords:
        return {}
    words = re.findall(r"\b\w+\b", body.lower())
    total = len(words)
    if not total:
        return {}
    density = {}
    for kw in keywords:
        kw_lower = kw.lower()
        count = words.count(kw_lower) if len(kw_lower.split()) == 1 else body.lower().count(kw_lower)
        density[kw] = round(count / total * 100, 2)
    return density


def _classify_page_type(url: str, body: str) -> str:
    """
    Rule-based classifier. Checks URL path (high precision) then body signals.
    Returns: product | article | category | homepage | unknown
    """
    path = urlparse(url).path.lower()
    if path in ("", "/", "/index.html", "/index.php"):
        return "homepage"

    body_lower = body.lower()[:500]
    scores: dict[str, int] = {t: 0 for t in _PAGE_TYPE_SIGNALS}

    for page_type, signals in _PAGE_TYPE_SIGNALS.items():
        for s in signals["url"]:
            if s in path:
                scores[page_type] += 3
        for s in signals["body"]:
            if s in body_lower:
                scores[page_type] += 1

    best = max(scores, key=lambda t: scores[t])
    return best if scores[best] > 0 else "unknown"
