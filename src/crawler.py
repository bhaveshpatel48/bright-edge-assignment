"""
Async HTTP crawler with:
- Per-domain rate limiting (token bucket via asyncio)
- Automatic retry with exponential backoff
- robots.txt compliance
- Redirect following + final URL normalization

Queue design: uses asyncio.Queue to model the fetch pipeline.
In production, replace asyncio.Queue with Kafka (confluent-kafka) or
SQS — the producer/consumer contract is identical.

  Producer  →  Queue[url]       →  Consumer fetches HTML
  Consumer  →  Queue[raw_page]  →  Parsing/NLP workers consume

Swap `asyncio.Queue` for a Kafka topic and the worker logic stays unchanged.
"""
from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from urllib.parse import urlparse, urlunparse
from urllib.robotparser import RobotFileParser

import aiohttp
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

log = structlog.get_logger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; BrightEdgeCrawler/1.0; "
        "+https://brightedge.com/crawler)"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
}

FETCH_TIMEOUT = aiohttp.ClientTimeout(total=15, connect=5)


def normalize_url(url: str) -> str:
    """Lowercase scheme+host, strip fragment for stable hashing."""
    parsed = urlparse(url)
    return urlunparse(parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower(),
        fragment="",
    ))


class DomainRateLimiter:
    """
    Token bucket per domain — 1 req/sec default (polite crawling).
    In production, Crawl-delay from robots.txt overrides this per domain.
    """

    def __init__(self, requests_per_second: float = 1.0):
        self._rps = requests_per_second
        self._last_call: dict[str, float] = defaultdict(float)
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def acquire(self, domain: str) -> None:
        async with self._locks[domain]:
            now = time.monotonic()
            wait = self._last_call[domain] + (1.0 / self._rps) - now
            if wait > 0:
                log.debug("rate_limit_wait", domain=domain, wait_s=round(wait, 2))
                await asyncio.sleep(wait)
            self._last_call[domain] = time.monotonic()


class RobotsCache:
    """Fetches and caches robots.txt per domain (O(domains), not O(URLs))."""

    def __init__(self):
        self._cache: dict[str, RobotFileParser] = {}

    async def is_allowed(self, session: aiohttp.ClientSession, url: str, user_agent: str) -> bool:
        domain = urlparse(url).netloc
        if domain not in self._cache:
            robots_url = f"{urlparse(url).scheme}://{domain}/robots.txt"
            rp = RobotFileParser()
            rp.set_url(robots_url)
            try:
                async with session.get(robots_url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        rp.parse((await resp.text()).splitlines())
                    else:
                        rp.allow_all = True
            except Exception:
                rp.allow_all = True
            self._cache[domain] = rp
            log.debug("robots_cached", domain=domain, allow_all=getattr(rp, "allow_all", False))

        return self._cache[domain].can_fetch(user_agent, url)


class Crawler:
    """
    Fetches a single URL. Designed to run as a queue worker:

        async with Crawler() as c:
            url = await url_queue.get()       # In prod: Kafka consumer
            result = await c.fetch(url)
            await raw_page_queue.put(result)  # In prod: Kafka producer
    """

    def __init__(self, rate_limiter: DomainRateLimiter | None = None):
        self._rate_limiter = rate_limiter or DomainRateLimiter()
        self._robots = RobotsCache()
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self):
        self._session = aiohttp.ClientSession(
            headers=DEFAULT_HEADERS,
            timeout=FETCH_TIMEOUT,
            connector=aiohttp.TCPConnector(limit=100, limit_per_host=2, ssl=False),
        )
        return self

    async def __aexit__(self, *args):
        if self._session:
            await self._session.close()

    async def fetch(self, url: str) -> tuple[str, str, int, int]:
        """
        Returns (final_url, html, status_code, duration_ms).
        Raises on non-retryable errors.
        """
        url = normalize_url(url)
        domain = urlparse(url).netloc

        allowed = await self._robots.is_allowed(self._session, url, DEFAULT_HEADERS["User-Agent"])
        if not allowed:
            log.warning("robots_blocked", url=url)
            return url, "", 403, 0

        await self._rate_limiter.acquire(domain)

        start = time.monotonic()
        html, status, final_url = await self._fetch_with_retry(url)
        duration_ms = int((time.monotonic() - start) * 1000)

        log.info("fetched", url=final_url, status=status, duration_ms=duration_ms)
        return normalize_url(final_url), html, status, duration_ms

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        reraise=True,
    )
    async def _fetch_with_retry(self, url: str) -> tuple[str, int, str]:
        async with self._session.get(url, allow_redirects=True) as resp:
            html = await resp.text(errors="replace")
            return html, resp.status, str(resp.url)


# ---------------------------------------------------------------------------
# Queue-based pipeline (mirrors Kafka producer/consumer in production)
# ---------------------------------------------------------------------------

async def run_pipeline(urls: list[str], concurrency: int = 10) -> list[dict]:
    """
    Demonstrates the queue-based pipeline pattern.

    asyncio.Queue  →  swap for Kafka topic in production
    worker tasks   →  swap for K8s Deployment pods in production

    Stages:
      url_queue      (urls-to-crawl topic)
      raw_page_queue (raw-pages topic) — consumed by extractor/NLP workers
    """
    from extractor import extract
    from nlp import enrich

    url_queue: asyncio.Queue[str | None] = asyncio.Queue()
    result_queue: asyncio.Queue[dict] = asyncio.Queue()
    rate_limiter = DomainRateLimiter()

    # Enqueue all URLs — in production: Ingest Service publishes to Kafka
    for url in urls:
        await url_queue.put(url)

    # Sentinel: one None per worker signals shutdown
    for _ in range(concurrency):
        await url_queue.put(None)

    log.info("pipeline_started", url_count=len(urls), workers=concurrency)

    async def worker(worker_id: int) -> None:
        """
        Crawler worker — consumes from url_queue, publishes to result_queue.
        In production: Kafka consumer group on 'urls-to-crawl' topic.
        """
        async with Crawler(rate_limiter=rate_limiter) as crawler:
            while True:
                url = await url_queue.get()
                if url is None:           # sentinel — worker shuts down
                    url_queue.task_done()
                    break
                try:
                    log.info("worker_processing", worker=worker_id, url=url)
                    final_url, html, status_code, duration_ms = await crawler.fetch(url)
                    page = extract(final_url, html, status_code, duration_ms)
                    page = enrich(page)
                    # In production: produce to 'enriched-pages' Kafka topic
                    await result_queue.put(page.to_dict())
                    log.info("worker_done", worker=worker_id, url=url, status=status_code)
                except Exception as exc:
                    log.error("worker_error", worker=worker_id, url=url, error=str(exc))
                    # In production: publish to 'crawl-errors' DLQ topic
                    result_queue.put_nowait({"url": url, "error": str(exc)})
                finally:
                    url_queue.task_done()

    workers = [asyncio.create_task(worker(i)) for i in range(concurrency)]
    await url_queue.join()
    await asyncio.gather(*workers)

    results = []
    while not result_queue.empty():
        results.append(await result_queue.get())

    log.info("pipeline_complete", total=len(results))
    return results
