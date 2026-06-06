"""
CLI entrypoint.

Commands:
  python main.py crawl <url>       — crawl one URL, print metadata, write crawl_result.json
  python main.py batch <file.txt>  — crawl all URLs via queue pipeline, write batch_results.json
  python main.py serve [port]      — start the FastAPI server (default port 8000)

The batch command uses the asyncio.Queue pipeline defined in crawler.py.
In production, replace the queue with Kafka — the worker logic is unchanged.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys

import structlog
import structlog.stdlib

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.dev.ConsoleRenderer(),   # swap for JSONRenderer() in production
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)

sys.path.insert(0, os.path.dirname(__file__))

from crawler import Crawler, run_pipeline
from extractor import extract
from nlp import enrich

log = structlog.get_logger(__name__)


async def crawl_one(url: str) -> dict:
    async with Crawler() as crawler:
        final_url, html, status_code, duration_ms = await crawler.fetch(url)
    page = extract(final_url, html, status_code, duration_ms)
    page = enrich(page)
    return page.to_dict()


def _print_result(data: dict) -> None:
    seo = data.get("seo", {})
    warnings = [
        k for k, v in {
            "title >60 chars": seo.get("title_too_long"),
            "meta-desc >160 chars": seo.get("meta_desc_too_long"),
            "thin content <300 words": seo.get("thin_content"),
            f"h1 count={seo.get('h1_count')} (expected 1)": seo.get("h1_count", 1) != 1,
        }.items() if v
    ]
    print("\n" + "=" * 60)
    print(f"URL         : {data.get('url')}")
    print(f"Title       : {data.get('title', '')[:80]}")
    print(f"Description : {data.get('meta_description', '')[:100]}")
    print(f"Page Type   : {data.get('page_type')}")
    print(f"Language    : {data.get('lang')}")
    print(f"Status      : {data.get('status_code')} ({data.get('crawl_duration_ms')}ms)")
    print(f"Words       : {seo.get('word_count')}")
    print(f"H1 Tags     : {data.get('h1_tags', [])[:3]}")
    print(f"H2 Tags     : {data.get('h2_tags', [])[:3]}")
    print(f"Canonical   : {data.get('canonical_url') or '(none)'}")
    print(f"OG Tags     : {seo.get('has_og_tags')}  |  Schema: {seo.get('has_schema_markup')}")
    print(f"Topics      : {data.get('topics', [])[:5]}")
    print(f"TF-IDF kws  : {[k for k, _ in (data.get('tfidf_keywords') or [])[:5]]}")
    print(f"SEO Warns   : {warnings or 'none'}")
    print("=" * 60 + "\n")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]

    if command == "crawl":
        if len(sys.argv) < 3:
            print("Usage: python main.py crawl <url>")
            sys.exit(1)
        url = sys.argv[2]
        log.info("crawl_start", url=url)
        result = asyncio.run(crawl_one(url))
        _print_result(result)
        with open("crawl_result.json", "w") as f:
            json.dump(result, f, indent=2, default=str)
        log.info("crawl_result_written", file="crawl_result.json")

    elif command == "batch":
        if len(sys.argv) < 3:
            print("Usage: python main.py batch <urls.txt>")
            sys.exit(1)
        with open(sys.argv[2]) as f:
            urls = [l.strip() for l in f if l.strip() and not l.startswith("#")]
        log.info("batch_start", url_count=len(urls))
        # run_pipeline uses asyncio.Queue — mirrors Kafka in production
        results = asyncio.run(run_pipeline(urls))
        with open("batch_results.json", "w") as f:
            json.dump(results, f, indent=2, default=str)
        log.info("batch_complete", result_count=len(results), file="batch_results.json")
        for r in results:
            _print_result(r)

    elif command == "serve":
        import uvicorn
        port = int(sys.argv[2]) if len(sys.argv) > 2 else 8000
        log.info("server_start", port=port)
        uvicorn.run("api:app", host="0.0.0.0", port=port, reload=False, app_dir=os.path.dirname(__file__))

    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
