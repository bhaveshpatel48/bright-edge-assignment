"""
FastAPI read layer — the "millions of requests on the content" path.

Design:
- In-process TTL cache simulates Redis. In production: swap TTLCache for Redis
  using the same key scheme (meta:{url_hash}) with identical TTL.
- GET /crawl   — on-demand crawl for demo. In production writes come from the
  async pipeline (Kafka consumers), not from this endpoint.
- GET /metadata/{url_hash} — retrieve pre-crawled results by URL hash.
- GET /health  — liveness + cache stats.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import sys

import structlog
from cachetools import TTLCache
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

sys.path.insert(0, os.path.dirname(__file__))

from crawler import Crawler, DomainRateLimiter, normalize_url
from extractor import extract
from nlp import enrich

log = structlog.get_logger(__name__)

app = FastAPI(
    title="BrightEdge SEO Crawler API",
    description="Crawl any URL and extract SEO metadata + topic classification.",
    version="1.0.0",
)

# In production: replace with Redis cluster (same key/TTL scheme)
_cache: TTLCache = TTLCache(maxsize=10_000, ttl=3600)
_cache_lock = asyncio.Lock()
_rate_limiter = DomainRateLimiter(requests_per_second=1.0)
_metrics = {"hits": 0, "misses": 0, "errors": 0, "total": 0}


@app.get("/health")
async def health():
    total = max(_metrics["total"], 1)
    return {
        "status": "ok",
        "cache_size": len(_cache),
        "cache_hit_rate_pct": round(_metrics["hits"] / total * 100, 1),
        **_metrics,
    }


@app.get("/crawl")
async def crawl_url(url: str = Query(..., description="URL to crawl")):
    """On-demand crawl for demo. In production, reads come from pre-crawled Cassandra data."""
    _metrics["total"] += 1
    normalized = normalize_url(url)
    url_hash = hashlib.sha256(normalized.encode()).hexdigest()

    async with _cache_lock:
        if url_hash in _cache:
            _metrics["hits"] += 1
            log.info("cache_hit", url=url, url_hash=url_hash[:12])
            return JSONResponse(content={**_cache[url_hash], "_cache": "HIT"})

    _metrics["misses"] += 1
    try:
        async with Crawler(rate_limiter=_rate_limiter) as crawler:
            final_url, html, status_code, duration_ms = await crawler.fetch(normalized)

        page = extract(final_url, html, status_code, duration_ms)
        page = enrich(page)
        result = page.to_dict()

        async with _cache_lock:
            _cache[url_hash] = result

        log.info("crawl_complete", url=url, status=status_code, page_type=page.page_type)
        return JSONResponse(content={**result, "_cache": "MISS"})

    except Exception as exc:
        _metrics["errors"] += 1
        log.error("crawl_failed", url=url, error=str(exc))
        raise HTTPException(status_code=502, detail=f"Crawl failed: {exc}")


@app.get("/metadata/{url_hash}")
async def get_metadata(url_hash: str):
    """Retrieve cached metadata by URL hash. In production: hits Cassandra."""
    _metrics["total"] += 1
    async with _cache_lock:
        data = _cache.get(url_hash)
    if not data:
        _metrics["misses"] += 1
        raise HTTPException(status_code=404, detail="Not found — crawl this URL first via /crawl")
    _metrics["hits"] += 1
    log.info("metadata_served", url_hash=url_hash[:12])
    return JSONResponse(content=data)
