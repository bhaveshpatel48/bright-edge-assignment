# Part 2 — System Design: Billion-URL Crawler at Scale

## Architecture

```
Text file / MySQL
        │
        ▼
INGESTION SERVICE  →  normalize_url()  →  Bloom Filter dedup  →  Kafka: urls-to-crawl (partition=domain)
        │
        ▼
CRAWLER WORKERS (aiohttp, K8s)  →  robots.txt cache (Redis)  →  rate limiter (1 req/s/domain)
        │
   ┌────┴────┐
   ▼         ▼
S3/GCS     Kafka: raw-pages
(raw HTML)       │
                 ├──▶ CHANGE DETECTOR (content_hash — skip NLP if unchanged, ~40% saved)
                 └──▶ PARSING SERVICE (trafilatura + BeautifulSoup)
                              │
                              ▼
                       NLP SERVICE (KeyBERT / TF-IDF, page type)
                              │
                              ▼
                       ENRICHMENT SERVICE ──▶ Cassandra (primary) + Elasticsearch (search)
                                                     │
                                   Kafka: crawl-errors (DLQ — retry with backoff)

READ PATH:
  Client → API Gateway → CDN Edge (~60% hit, <5ms) → Redis (~25% hit, ~1ms) → Cassandra (~15%, ~5ms)
```

## Storage Choices

| Store | Role | Why |
|---|---|---|
| **Cassandra** | Primary metadata KV | Linear write scale; O(1) by `url_hash`; row TTL 90 days; no hot spots |
| **Elasticsearch** | Full-text + topic search | Aggregations by domain/topic/page_type that Cassandra can't serve |
| **Redis** | Hot-URL read cache + Bloom Filter + robots cache | 70–80% of CDN-miss reads served in ~1ms |
| **CDN** | Edge cache (CloudFront/Cloudflare) | ~60% of read traffic at <5ms globally; no Redis/Cassandra hit |
| **S3/GCS** | Raw HTML | Object storage is 10× cheaper than DB for large blobs; rarely re-read |

**Why not MySQL:** B-tree index degrades past ~500M rows; single-leader caps writes; billion-row schema migrations take hours.

## Kafka Topics

| Topic | Partition Key | Retention | Purpose |
|---|---|---|---|
| `urls-to-crawl` | domain | 7 days | Domain affinity = natural per-domain rate limiting |
| `raw-pages` | url_hash | 3 days | Fan-out to Parsing + Change Detector |
| `parsed-pages` | url_hash | 3 days | NLP input |
| `enriched-pages` | url_hash | 3 days | Cassandra/ES writers |
| `crawl-errors` | domain | 30 days | DLQ — retry 1→5m, 2→30m, 3→2h, 4→24h, 5→dead |

## Idempotency (6 Control Points)

| # | Where | Mechanism |
|---|---|---|
| 1 | Ingest | `normalize_url()` → stable `url_hash = SHA-256(url)` |
| 2 | Ingest | Bloom Filter `BF.EXISTS/BF.ADD` — skip already-queued URLs |
| 3 | Crawl | `robots.txt` cached per domain in Redis (TTL=24h) |
| 4 | Storage | Cassandra `INSERT` = always upsert on `(url_hash, crawled_at)` |
| 5 | Pipeline | `content_hash` — skip NLP if body unchanged (~40% compute saved) |
| 6 | Kafka | `enable.auto.commit=false`; offset committed only after Cassandra write |

## Read-Heavy Matching at Scale

**Problem:** Millions of read requests/sec for content lookup, topic filtering, and keyword search simultaneously.

| Layer | Technology | Role | Throughput |
|---|---|---|---|
| **CDN edge** | CloudFront / Cloudflare | Serve cached metadata JSON by `url_hash` at edge | ~60% of reads, <5ms globally |
| **Redis cluster** | Cluster mode, 3+ shards | Hot-URL cache (`meta:{url_hash}`), topic→url reverse index | ~25% of CDN-miss reads, ~1ms |
| **Elasticsearch** | 3-node cluster, replicas=2 | Full-text keyword search, topic aggregations, faceted filtering | ~15% of reads, <20ms p99 |
| **Cassandra** | Multi-node, RF=3 | Primary KV lookup by `url_hash` when cache cold | O(1) by partition key, ~5ms |

**Keyword / Topic search path:**
```
Client → CDN (miss) → Redis ZSET (topic→url_hash sorted by score, miss)
       → Elasticsearch (BM25 keyword match + topic filter aggregation)
       → Response cached in Redis (TTL=5min) + CDN (TTL=1h)
```

**High-read design choices:**
- Redis `ZSET` keyed `topic:{topic_name}` stores top-1000 URLs sorted by relevance score — O(log N) range reads
- ES index sharded by `page_type` so topic queries fan out only to relevant shards
- Read replicas on Cassandra (LOCAL_ONE consistency) — reads never hit coordinator
- Response payloads gzip-compressed at CDN; p99 payload <2KB for metadata, <10KB with body_text

**Scale math (reads):**
- 1M reads/sec: ~600K served at CDN edge, ~250K from Redis, ~150K from ES/Cassandra
- Redis: 250K ops/sec → 3 shards at ~80K ops/sec each (well within 100K/shard limit)
- ES: 150K reads/sec → 3 nodes at ~50K/sec each (scale to 6 nodes at 300K reads/sec peak)

## SLOs / SLAs

| Metric | Internal SLO | Customer SLA (Enterprise) |
|---|---|---|
| Crawl freshness | >95% URLs crawled within 30 days | — |
| Crawl success rate | >98% HTTP 200 | — |
| API p50 latency | <10ms | — |
| API p99 latency | <100ms | <100ms |
| API availability | >99.9% monthly | >99.9% monthly |
| NLP lag behind crawl | <4 hours | — |

## Monitoring

| Layer | Metric | Alert Threshold |
|---|---|---|
| Crawl | `urls_per_second` | <200/sec for >5min |
| Crawl | `success_rate` | <97% |
| Kafka | consumer lag | >1M messages |
| NLP | `processing_lag_seconds` | >4 hours |
| Cassandra | `write_latency_p99` | >50ms |
| Redis | `cache_hit_rate` | <35% |
| API | `5xx error rate` | >0.1% |
| CDN | `cache_hit_rate` | <45% |

**Tools:** New Relic (metrics + logs + traces + alerts, all-in-one APM platform).

**Why New Relic over PagerDuty + Prometheus + Grafana stack:**

| Capability | New Relic | PagerDuty + Grafana + Prometheus |
|---|---|---|
| Metrics | Built-in (NRDB) | Prometheus + Grafana (separate setup) |
| Logs | Built-in log forwarding | ELK stack (additional cost + ops) |
| Distributed traces | Built-in (APM) | Jaeger (additional setup) |
| Alerting + on-call | Built-in alert policies → PagerDuty optional | PagerDuty ($19–$59/user/mo) separate |
| Cost (100 hosts) | ~$0.30/GB data ingested; single vendor | Grafana Cloud ~$8/host + PagerDuty ~$25/user/mo + ELK hosting |
| Setup complexity | Single agent, one API key | 4+ separate tools, 4+ data pipelines |

**Cost efficiency verdict:** New Relic is cheaper and simpler when you factor in PagerDuty seats + ELK hosting + Grafana Cloud + Prometheus storage. Single NRQL query language across metrics, logs, and traces eliminates context-switching. At startup/PoC scale, New Relic's free tier (100GB/mo) covers the entire PoC with zero cost.

## Cost Optimizations

| Strategy | Saving |
|---|---|
| Change detection (skip unchanged pages) | ~40% NLP compute |
| Spot/Preemptible instances for crawlers | ~70% compute cost |
| S3 Intelligent-Tiering for raw HTML | ~40% storage cost |
| CDN edge cache | ~60% of reads served at edge |
| GPU batch inference for KeyBERT | 10× NLP throughput → fewer nodes |

## Scale Math

- Target: 1B URLs/month = ~385 URLs/sec sustained
- Crawler workers: 1 req/s/domain × 100 domains → 4 workers (run 10 for headroom)
- NLP on CPU: ~200ms/page → 77 workers. GPU: ~20ms/page → 8 GPUs. With 40% skip → 5 GPUs.

---
*Full DB schemas (Cassandra DDL, ES mappings, Redis key design) → [idempotency_and_db_schema.md](idempotency_and_db_schema.md)*
*Architecture Mermaid diagrams → [architecture_visual.md](architecture_visual.md)*
