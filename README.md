# BrightEdge Engineering Candidate Assignment — Scale

**Candidate:** Bhavesh Vaviya | **Date:** 2026-06-06

**AI Tools Used:** Claude (Anthropic) — architecture review, DB tradeoff reasoning, design doc structure. GitHub Copilot — boilerplate completion. All design decisions authored by candidate.

---

## Live Demo (Deployed on Render)

The service is deployed at **https://bright-edge-assignment.onrender.com** on Render's free plan.

> **Note:** The free plan spins down the instance after inactivity. The **first request after a period of inactivity may take 30–60 seconds** while the instance boots. Subsequent requests will be fast.

### Health check

```bash
curl "https://bright-edge-assignment.onrender.com/health"
```

### Crawl a URL

```bash
curl "https://bright-edge-assignment.onrender.com/crawl?url=https://www.brightedge.com/"
```

### Fetch cached metadata by URL hash

```bash
# SHA-256 hash of https://www.brightedge.com/
curl "https://bright-edge-assignment.onrender.com/metadata/5b614334b897c6967241576c980363aa6acf66019bc556c5de7a64e61c4817ea"
```

> The hash above corresponds to `https://www.brightedge.com/`. To get the hash for any other URL, hit `/crawl?url=<url>` first — the response includes `url_hash`.

---

## Part 1 — Core Crawler

Given any URL, returns: title, meta description, canonical URL, Open Graph tags, H1/H2/H3 headings, clean body text, page type, topics, keyword density, SEO flags, internal/external link counts, language.

**Queue design:** `crawler.py` uses `asyncio.Queue` to model a producer/consumer pipeline. In production, swap the queue for Kafka — the worker logic is unchanged. See `run_pipeline()` in [src/crawler.py](src/crawler.py).

### Setup

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cd src
```

> First run downloads KeyBERT model `all-MiniLM-L6-v2` (~90MB, cached in `~/.cache/huggingface/`).

### Run

```bash
# Single URL
python main.py crawl "https://blog.rei.com/camp/how-to-introduce-your-indoorsy-friend-to-the-outdoors/"
python main.py crawl "https://www.cnn.com/2025/09/23/tech/google-study-90-percent-tech-jobs-ai"

# Batch (queue pipeline)
python main.py batch ../test_urls.txt

# REST API  →  http://localhost:8000/docs
python main.py serve
```

**API endpoints:**

| Endpoint | Description |
|---|---|
| `GET /crawl?url=` | On-demand crawl, returns full metadata |
| `GET /metadata/{url_hash}` | Cached result by SHA-256(url) |
| `GET /health` | Liveness + cache hit rate |

### Sample Output

```
URL         : https://blog.rei.com/camp/how-to-introduce-your-indoorsy-friend-to-the-outdoors/
Title       : How to Introduce Your Indoorsy Friend to the Outdoors
Description : Getting a friend outside for the first time...
Page Type   : article
Language    : en
Status      : 200 (1340ms)
Words       : 1240
H1 Tags     : ['How to Introduce Your Indoorsy Friend to the Outdoors']
Topics      : ['outdoor activities', 'hiking tips', 'nature walk', 'camping gear', 'trail advice']
OG Tags     : True  |  Schema: True
SEO Warns   : none
```

Full JSON written to `crawl_result.json`.

### Key Design Choices

| Decision | Reason |
|---|---|
| `aiohttp` async | I/O-bound crawling; 1000 threads = ~8GB stack RAM; async = ~10MB |
| `trafilatura` for body | Removes nav/footer/ads (~90% accuracy); raw `get_text()` includes boilerplate |
| KeyBERT for topics | Semantic embeddings; TF-IDF misses synonyms. TF-IDF used as fallback |
| `content_hash` per page | ~40% of pages unchanged month-to-month; skip NLP to save compute |
| `asyncio.Queue` | Mirrors Kafka contract; swap queue for topic, workers become K8s pods |
| `structlog` | Structured key=value logs; swap `ConsoleRenderer` for `JSONRenderer` in prod |

### Known Limitations

| Limitation | Production Fix |
|---|---|
| Amazon blocks bots | Playwright headless browser fallback for JS pages |
| KeyBERT ~400ms/page on CPU | GPU batch inference (10× throughput) |
| In-process cache (not shared) | Redis cluster; same `meta:{url_hash}` key scheme |
| No persistent storage | Cassandra write path (Phase 4 of PoC) |

---

## Part 2 — Scale Design

**→ [docs/part2_system_design.md](docs/part2_system_design.md)**

Pipeline: Text file / MySQL → Ingestion (Bloom Filter dedup) → Kafka `urls-to-crawl` → Crawler Workers → Kafka `raw-pages` → Parsing → NLP → Cassandra + Elasticsearch + Redis + CDN.

| Layer | Technology | Why |
|---|---|---|
| Message queue | Kafka (partitioned by domain) | Domain affinity = natural per-domain rate limiting |
| Primary store | Cassandra | Linear write scale; O(1) by `url_hash`; built-in TTL |
| Search | Elasticsearch | Full-text + topic aggregations Cassandra can't serve |
| Hot cache | Redis → CDN | 85–90% total read cache hit; CDN serves ~60% at <5ms |
| Dedup | Redis Bloom Filter | 1B URLs, 1% FPR → 1.2GB RAM |

**Read-heavy matching (millions of reads/sec):** CDN handles ~60%, Redis ZSET (`topic:{name}` → sorted URL list) handles ~25%, Elasticsearch BM25 keyword + topic-filter queries handle the remaining ~15%. Scale math: 1M reads/sec → 3 Redis shards at 80K ops/sec each, 3–6 ES nodes at 50–100K reads/sec each.

**Observability:** New Relic (single agent — metrics + logs + traces + alerts). More cost-efficient than PagerDuty + Prometheus + Grafana + ELK stack; free tier covers the full PoC (100GB/mo).

**SLOs:** crawl freshness >95% in 30 days, API p99 <100ms, availability >99.9%.

Supporting detail: [docs/idempotency_and_db_schema.md](docs/idempotency_and_db_schema.md) | [docs/architecture_visual.md](docs/architecture_visual.md)

---

## Part 3 — PoC Plan & Estimates

**→ [docs/part3_poc_plan.md](docs/part3_poc_plan.md)**

| Phase | Scope | Duration |
|---|---|---|
| 1 | Crawler + extraction | Week 1 |
| 2 | NLP pipeline | Week 1–2 |
| 3 | API + cache | Week 2 |
| 4 | Kafka + Cassandra + deploy to AWS/GCP | Week 3 |
| 5 | Monitoring, hardening, stakeholder demo | Week 4 |

**Top blockers:** JS-rendered pages (Playwright +3 days), Cassandra ops (use AWS Keyspaces for PoC), KeyBERT CPU latency (GPU deferred post-PoC).

**PoC pass criteria:** >97% crawl success rate, API p99 <100ms, 100K URLs with 0 data loss on restart.
