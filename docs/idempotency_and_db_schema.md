# Idempotency Design + Full Database Schemas

---

## Part 1 — How Idempotency Works End-to-End

### What "Idempotent Crawl" Means

Crawling the same URL twice should produce the same stored state, not a duplicate row or a
corrupted entry. This is hard because writes flow through multiple async stages
(Kafka → Crawler → Parsing → NLP → Cassandra/ES). Any stage can fail and retry.

---

### Idempotency at Each Layer

```
┌─────────────────────────────────────────────────────────────────────┐
│                    IDEMPOTENCY CONTROL POINTS                        │
│                                                                       │
│  [1] INGEST          [2] DEDUP           [3] CRAWL                   │
│  URL normalization   Bloom Filter        robots.txt cache             │
│  → stable url_hash   → skip seen URLs   → stable per-domain          │
│                                                                       │
│  [4] STORAGE         [5] CHANGE          [6] KAFKA                   │
│  Cassandra upsert    DETECTION           Exactly-once                 │
│  on PK (url_hash,    content_hash        semantics                    │
│  crawled_at)         → skip NLP if same  (idempotent offset commits)  │
└─────────────────────────────────────────────────────────────────────┘
```

---

### Control Point 1 — URL Normalization → Stable Primary Key

Before anything is queued, the URL is normalized to a canonical form:

```python
def normalize_url(url: str) -> str:
    parsed = urlparse(url.lower().strip())
    # Strip fragment (#section), normalize trailing slash
    normalized = parsed._replace(fragment="", path=parsed.path.rstrip("/") or "/")
    return urlunparse(normalized)

url_hash = sha256(normalize_url(url).encode()).hexdigest()
```

**Why this matters**: `https://EXAMPLE.com/Page#section` and `https://example.com/page`
are the same page. Without normalization, both get crawled, produce duplicate rows, and
waste ~10–15% of crawl budget on canonical duplicates.

The `url_hash` is the immutable identity of a URL. Everything downstream keys on it.

---

### Control Point 2 — Bloom Filter Deduplication at Ingest

```
URL arrives → normalize → url_hash
                              │
                    BF.EXISTS url_hash?
                    ├── YES → drop (already queued this run)
                    └── NO  → BF.ADD url_hash → enqueue to Kafka
```

**Properties**:
- No false negatives: a URL we've already queued will never be re-queued
- ~1% false positives: a small fraction of new URLs are incorrectly skipped (acceptable)
- Bloom filter is per-crawl-run: reset at the start of each monthly crawl cycle
- 1B URLs, 1% FPR → ~1.2GB RAM (RedisBloom module)

**This is the primary dedup fence** — it stops duplicate work before it enters the pipeline.

---

### Control Point 3 — robots.txt Cache (Per-Domain, TTL=24h)

```python
# Fetched once per domain per crawl run
robots_cache: dict[str, RobotFileParser] = {}  # backed by Redis in production

async def is_allowed(url: str) -> bool:
    domain = urlparse(url).netloc
    if domain not in robots_cache:
        robots_cache[domain] = await fetch_robots(domain)
    return robots_cache[domain].can_fetch("*", url)
```

Without this cache, every URL would fetch `robots.txt` — 1B fetches for `robots.txt`
before any real crawl happens. Cache makes this O(domains), not O(URLs).

---

### Control Point 4 — Cassandra Upsert (Idempotent Writes)

Cassandra's `INSERT` is always an upsert — it overwrites on primary key conflict.
Our primary key is `(url_hash, crawled_at)`.

```cql
-- This is always safe to retry. If it runs twice with the same data,
-- the second write simply overwrites with identical values.
INSERT INTO url_metadata (url_hash, crawled_at, title, topics, ...)
VALUES (?, ?, ?, ?, ...)
USING TTL 7776000;
```

**Key point**: Kafka consumers use **at-least-once delivery**. A network blip means the
same `enriched-pages` message is processed twice. Because Cassandra writes are idempotent
(same PK = overwrite, not append), this is safe. The row ends up identical either way.

**For truly exactly-once semantics** (Kafka → Cassandra):
- Use Kafka consumer `enable.auto.commit=false`
- Commit offset only after Cassandra `INSERT` succeeds
- If the INSERT fails → do not commit → message is redelivered → INSERT retried (safe because idempotent)

```
Consumer reads message
    │
    ├── Cassandra INSERT (success)
    │       └── Kafka commit offset ✓
    │
    └── Cassandra INSERT (failure)
            └── do NOT commit offset
                    └── message redelivered on restart → retry INSERT
```

---

### Control Point 5 — Content Hash Change Detection (Skip Idempotency)

Even when we re-crawl a URL (scheduled monthly refresh), we check if the page actually changed:

```python
new_content_hash = sha256(body_text.encode()).hexdigest()

# Check stored hash in Redis (fast) or Cassandra (slow fallback)
stored_hash = await redis.get(f"content_hash:{url_hash}")

if new_content_hash == stored_hash:
    # Page unchanged — write crawl_timestamp only, skip NLP pipeline
    await cassandra.execute(
        "UPDATE url_metadata SET crawled_at=? WHERE url_hash=?",
        [now, url_hash]
    )
    return  # No Kafka publish to parsed-pages
```

**Effect**: ~40% of pages are unchanged month-to-month. Skipping NLP for them:
- Saves GPU compute
- Keeps Cassandra data stable (no meaningless overwrites of topics/keywords)
- Is itself idempotent (running the check twice gives the same result)

---

### Control Point 6 — Kafka Exactly-Once for Pipeline Stages

Each Kafka consumer (Parsing Service, NLP Service, Enrichment Service) uses:

```
Consumer group offset  → tracks position per partition
enable.auto.commit=false  → manual commit after processing
idempotent producer        → broker deduplicates producer retries by sequence number
transactions               → wrap consume + produce + offset commit atomically
```

The pipeline uses **read-process-write transactions**:

```
BEGIN TRANSACTION
  consume message from raw-pages partition P at offset O
  parse content → produce to parsed-pages
  commit offset O for partition P
END TRANSACTION
```

If the worker crashes mid-transaction, Kafka rolls back. The message is redelivered.
The Parsing Service then re-parses (idempotent) and re-publishes the same output.

---

### Summary: What Idempotency Buys You

| Without Idempotency | With Idempotency |
|---|---|
| Worker crash → URL crawled twice → duplicate rows | Worker crash → URL reprocessed → same row |
| Same URL submitted twice → two crawl jobs | Bloom Filter drops second → one crawl job |
| NLP failure → partial data in Cassandra | NLP retried → Cassandra write overwrites safely |
| Network blip → Kafka message processed twice | Cassandra upsert → identical result |
| Page unchanged → 100% NLP compute every run | Content hash → 40% NLP skipped |

At 1B URLs/month, the difference between "probably idempotent" and "provably idempotent" is
the difference between a system that drifts and corrupts over time and one that self-heals.

---

---

## Part 2 — Visual Architecture Diagram

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║                    BRIGHTEDGE BILLION-URL CRAWLER — FULL ARCHITECTURE           ║
╚══════════════════════════════════════════════════════════════════════════════════╝

  INPUT SOURCES
  ┌─────────────┐    ┌─────────────┐
  │  Text File  │    │   MySQL DB  │
  │ (line-by-   │    │ (batch 10K) │
  │  line gen)  │    │             │
  └──────┬──────┘    └──────┬──────┘
         └────────┬──────────┘
                  ▼
  ┌───────────────────────────────────────────────────┐
  │              INGESTION SERVICE                     │
  │                                                    │
  │  1. normalize_url() → url_hash = SHA-256(url)     │
  │  2. BF.EXISTS url_hash → if seen: DROP            │  ← idempotency fence #1
  │  3. BF.ADD url_hash                               │
  │  4. Publish to Kafka: urls-to-crawl               │
  │     partition_key = domain                        │  ← domain affinity
  └───────────────────────────────────────────────────┘
                  │
                  ▼
  ┌───────────────────────────────────────────────────────────────────┐
  │        KAFKA: urls-to-crawl   (partition_key = domain)            │
  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
  │  │ Partition 0 │  │ Partition 1 │  │ Partition N │              │
  │  │ amazon.com  │  │ google.com  │  │ cnn.com ... │              │
  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘              │
  └─────────┼─────────────────┼─────────────────┼───────────────────┘
            └────────┬─────────┘─────────────────┘
                     ▼ (each worker owns a subset of domain partitions)
  ┌───────────────────────────────────────────────────────────────────┐
  │              CRAWLER WORKERS  (K8s Deployment, auto-scale)        │
  │                                                                   │
  │  • aiohttp async, TCPConnector(limit=100, limit_per_host=2)       │
  │  • DomainRateLimiter: 1 req/sec/domain (token bucket)            │
  │  • RobotsCache: fetch once per domain, TTL=24h → Redis           │  ← idempotency fence #3
  │  • Retry: tenacity 3× exp backoff (1s → 10s)                     │
  │                                                                   │
  │  On success:  HTML + status_code + duration_ms                    │
  │  On failure:  publish to Kafka: crawl-errors (DLQ)               │
  └──────────────────────┬────────────────────────────────────────────┘
                         │
           ┌─────────────┴─────────────┐
           ▼                           ▼
  ┌──────────────────┐    ┌──────────────────────────────────────┐
  │  S3 / GCS        │    │  KAFKA: raw-pages  (key=url_hash)    │
  │  Raw HTML        │    │  { url, url_hash, html, status_code, │
  │  (gzipped)       │    │    crawled_at, duration_ms }         │
  │  key=url_hash    │    └────────────┬─────────────────────────┘
  │  path=yr/mo/dom/ │                 │
  └──────────────────┘        ┌────────┴────────┐
                              │                 │
                              ▼                 ▼
             ┌────────────────────────┐   ┌───────────────────────────────┐
             │   CHANGE DETECTOR      │   │   PARSING SERVICE             │
             │                        │   │                               │
             │ content_hash =         │   │ • trafilatura (primary)       │
             │  SHA-256(body_text)    │   │   content extraction          │
             │                        │   │ • BeautifulSoup (lxml)        │
             │ Redis GET              │   │   metadata: title, meta,      │
             │  content_hash:{hash}   │   │   h1-h3, OG, canonical,       │
             │                        │   │   JSON-LD, links              │
             │ if same → UPDATE       │   │ • SEO flags: thin content,    │
             │  crawled_at only       │   │   title length, meta length   │  ← idempotency fence #5
             │  skip NLP (~40% skip) │   │                               │
             │                        │   │ → Kafka: parsed-pages         │
             └────────────────────────┘   └──────────────┬────────────────┘
                                                         │
                                                         ▼
                                          ┌──────────────────────────────────┐
                                          │    NLP SERVICE  (GPU workers)    │
                                          │                                  │
                                          │ • KeyBERT (all-MiniLM-L6-v2)    │
                                          │   keyphrases + MMR diversity     │
                                          │ • TF-IDF (sklearn) fallback      │
                                          │ • keyword_density computation    │
                                          │ • page_type classifier           │
                                          │   (URL path + body signals)      │
                                          │                                  │
                                          │ → Kafka: enriched-pages          │
                                          └──────────────┬───────────────────┘
                                                         │
                                                         ▼
                                          ┌──────────────────────────────────┐
                                          │    ENRICHMENT SERVICE            │
                                          │                                  │
                                          │ • Assemble PageMetadata          │
                                          │ • Dual write:                    │
                                          │   ├── Cassandra (primary, durab) │  ← idempotency fence #4
                                          │   └── Elasticsearch (write-behi) │
                                          │ • Redis SET content_hash:{hash}  │
                                          │                                  │
                                          │ On failure →                     │
                                          │   Kafka: crawl-errors (DLQ)     │
                                          └──────────────────────────────────┘

  ════════════════════════════════════════════════════════════════════════
                    RETRY / DLQ PATTERN
  ════════════════════════════════════════════════════════════════════════

  ┌──────────────────────────────────────────────────────────────────┐
  │  KAFKA: crawl-errors  (partition_key = domain)                   │
  │  Retention: 30 days                                              │
  │                                                                  │
  │  Retry Consumer reads, checks retry_count header:               │
  │    retry_count 1 → delay 5 min   → re-publish urls-to-crawl    │
  │    retry_count 2 → delay 30 min  → re-publish urls-to-crawl    │
  │    retry_count 3 → delay 2 hours → re-publish urls-to-crawl    │
  │    retry_count 4 → delay 24 hours                               │
  │    retry_count 5 → dead letter (alert, manual review)           │
  └──────────────────────────────────────────────────────────────────┘

  ════════════════════════════════════════════════════════════════════
                    READ PATH
  ════════════════════════════════════════════════════════════════════

  Client Request (GET /metadata/{url_hash})
           │
           ▼
  ┌────────────────────────────────────────────────────────────────┐
  │  API GATEWAY  (rate limiting, auth, routing)                   │
  │  Write path → Ingest API                                       │
  │  Read path  → Query API                                        │
  └──────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
  ┌────────────────────────────────────────┐
  │   CDN EDGE CACHE (CloudFront / CF)     │   ← Tier 1 Cache
  │   Cache-Control: max-age=3600          │
  │   stale-while-revalidate: 86400        │
  │   Cache key: /metadata/{url_hash}      │
  │   ~50–70% hit rate, <5ms globally      │
  └────────────────────┬───────────────────┘
                  CDN MISS │
                         ▼
  ┌────────────────────────────────────────┐
  │   REDIS CLUSTER (sharded, 3-node)      │   ← Tier 2 Cache
  │   Key:  meta:{url_hash}                │
  │   Value: JSON(PageMetadata)            │
  │   TTL:  3600s                          │
  │   ~70–80% hit rate of CDN misses       │
  │   → total cache hit rate ~85–90%       │
  └────────────────────┬───────────────────┘
               Redis MISS │
                         ▼
  ┌────────────────────────────────────────┐
  │   CASSANDRA                            │   ← Source of Truth
  │   SELECT WHERE url_hash = ?           │
  │   CONSISTENCY QUORUM                   │
  │   ~5ms point lookup                    │
  │   → populate Redis + CDN on return     │
  └────────────────────────────────────────┘

  ════════════════════════════════════════════════════════════════════
                    MONITORING LAYER
  ════════════════════════════════════════════════════════════════════

  Prometheus (scrapes all services)
       │
       ▼
  Grafana Dashboards:
  ├── Crawl Throughput   (urls/sec, success rate, error breakdown)
  ├── Pipeline Lag       (Kafka consumer lag, NLP processing lag)
  ├── API SLO            (p50/p99 latency vs. SLO lines)
  └── Storage Health     (Cassandra latency, Redis hit rate)

  ELK Stack → structured JSON logs from all services
  Jaeger   → distributed trace: URL submission → Cassandra write
  PagerDuty → alerts on SLO breach (crawl rate, API latency, error rate)
```

---

---

## Part 3 — Full Database Schema Definitions

### 3.1 Cassandra — Primary Metadata Store

**Schema design principles:**
- Partition key = `url_hash` → O(1) point lookup, even distribution across nodes
- Clustering key = `crawled_at DESC` → "latest crawl for this URL" is the first row
- All complex types are native Cassandra collections (LIST, MAP, UDT)
- TTL = 90 days on every row — no cleanup job needed

```cql
-- ────────────────────────────────────────────────────────────
-- KEYSPACE
-- ────────────────────────────────────────────────────────────
CREATE KEYSPACE brightedge
  WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'us-east-1': 3,         -- 3 replicas per region
    'eu-west-1': 3
  }
  AND durable_writes = true;

USE brightedge;

-- ────────────────────────────────────────────────────────────
-- USER DEFINED TYPE: seo_insights
-- Embedded in url_metadata to avoid a join
-- ────────────────────────────────────────────────────────────
CREATE TYPE seo_insights (
    title_length         INT,
    meta_desc_length     INT,
    h1_count             INT,
    has_canonical        BOOLEAN,
    has_og_tags          BOOLEAN,
    has_schema_markup    BOOLEAN,
    internal_link_count  INT,
    external_link_count  INT,
    word_count           INT,
    keyword_density      MAP<TEXT, FLOAT>,  -- {"seo tools": 2.3, "crawl": 1.1}
    title_too_long       BOOLEAN,
    meta_desc_too_long   BOOLEAN,
    thin_content         BOOLEAN
);

-- ────────────────────────────────────────────────────────────
-- PRIMARY TABLE: url_metadata
-- One row per crawl event. PK: (url_hash, crawled_at).
-- Read "latest" = LIMIT 1 with DESC clustering order.
-- ────────────────────────────────────────────────────────────
CREATE TABLE url_metadata (
    -- Identity
    url_hash             TEXT,
    crawled_at           TIMESTAMP,          -- clustering, DESC
    url                  TEXT,
    domain               TEXT,

    -- HTML Metadata
    title                TEXT,
    meta_description     TEXT,
    meta_keywords        LIST<TEXT>,
    canonical_url        TEXT,
    lang                 TEXT,               -- 'en', 'de', 'fr', ...

    -- Structural Signals
    h1_tags              LIST<TEXT>,
    h2_tags              LIST<TEXT>,
    h3_tags              LIST<TEXT>,
    og_title             TEXT,
    og_description       TEXT,
    og_image             TEXT,

    -- Content
    content_hash         TEXT,               -- SHA-256(body_text) for change detection
    body_text_preview    TEXT,               -- first 500 chars only; full text in S3

    -- NLP Outputs
    topics               LIST<TEXT>,         -- KeyBERT keyphrases (top 10)
    page_type            TEXT,               -- product | article | category | homepage | unknown
    tfidf_keywords       MAP<TEXT, FLOAT>,   -- {"outdoor gear": 4.2, "camping": 3.1}

    -- SEO Analysis (embedded UDT — no join needed)
    seo                  FROZEN<seo_insights>,

    -- Crawl Diagnostics
    status_code          INT,
    crawl_duration_ms    INT,
    error_message        TEXT,               -- null if success

    -- Storage Pointer
    s3_raw_path          TEXT,               -- s3://bucket/yr/mo/domain/url_hash.html.gz

    PRIMARY KEY (url_hash, crawled_at)
)
WITH CLUSTERING ORDER BY (crawled_at DESC)
  AND default_time_to_live = 7776000        -- 90 days
  AND compaction = {
    'class': 'LeveledCompactionStrategy',   -- good for read-heavy, predictable latency
    'sstable_size_in_mb': 160
  }
  AND compression = {
    'class': 'LZ4Compressor'                -- fast CPU-cheap compression
  }
  AND caching = {
    'keys': 'ALL',
    'rows_per_partition': '10'              -- cache up to 10 crawl versions per URL
  };

-- ────────────────────────────────────────────────────────────
-- SECONDARY TABLE: urls_by_domain
-- Allows "give me all URLs crawled for amazon.com this month"
-- This is a query pattern Cassandra can't serve from url_metadata
-- because domain is not in the partition key.
-- ────────────────────────────────────────────────────────────
CREATE TABLE urls_by_domain (
    domain               TEXT,
    crawled_at           TIMESTAMP,
    url_hash             TEXT,
    url                  TEXT,
    page_type            TEXT,
    status_code          INT,
    PRIMARY KEY (domain, crawled_at, url_hash)
)
WITH CLUSTERING ORDER BY (crawled_at DESC)
  AND default_time_to_live = 7776000;

-- ────────────────────────────────────────────────────────────
-- SECONDARY TABLE: content_hashes
-- Fast lookup for Change Detector: "has this page changed?"
-- Kept small — only the hash, no metadata.
-- ────────────────────────────────────────────────────────────
CREATE TABLE content_hashes (
    url_hash             TEXT PRIMARY KEY,
    content_hash         TEXT,
    last_crawled_at      TIMESTAMP
)
WITH default_time_to_live = 7776000;


-- ────────────────────────────────────────────────────────────
-- EXAMPLE QUERIES
-- ────────────────────────────────────────────────────────────

-- Latest metadata for a URL (O(1) by url_hash):
SELECT * FROM url_metadata
WHERE url_hash = 'abc123def...'
LIMIT 1;

-- All crawl history for a URL (last 5):
SELECT crawled_at, status_code, content_hash, page_type
FROM url_metadata
WHERE url_hash = 'abc123def...'
LIMIT 5;

-- All URLs crawled for amazon.com in the last 7 days:
SELECT url, page_type, status_code
FROM urls_by_domain
WHERE domain = 'amazon.com'
  AND crawled_at >= '2026-06-01'
LIMIT 1000;
```

---

### 3.2 Elasticsearch — Full-Text and Topic Search Index

**Index design principles:**
- Separate analyzed fields (full-text search) from keyword fields (exact match + aggregations)
- `body_text` is `text` (analyzed) — enables `match`, `more_like_this`, phrase search
- `topics`, `page_type`, `domain`, `lang` are `keyword` — enables aggregations + exact filters
- `tfidf_keywords` is a nested type — enables "pages with keyword X above density Y"

```json
PUT /page_metadata
{
  "settings": {
    "number_of_shards": 10,
    "number_of_replicas": 1,
    "analysis": {
      "analyzer": {
        "seo_analyzer": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": ["lowercase", "stop", "snowball"]
        }
      }
    },
    "index.refresh_interval": "30s"
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {

      "url_hash":          { "type": "keyword" },
      "url":               { "type": "keyword" },
      "domain":            { "type": "keyword" },
      "crawled_at":        { "type": "date" },

      "title": {
        "type": "text",
        "analyzer": "seo_analyzer",
        "fields": {
          "raw": { "type": "keyword" }
        }
      },

      "meta_description":  { "type": "text", "analyzer": "seo_analyzer" },
      "meta_keywords":     { "type": "keyword" },
      "canonical_url":     { "type": "keyword" },
      "lang":              { "type": "keyword" },

      "h1_tags":           { "type": "text", "analyzer": "seo_analyzer" },
      "h2_tags":           { "type": "text", "analyzer": "seo_analyzer" },

      "body_text": {
        "type": "text",
        "analyzer": "seo_analyzer",
        "term_vector": "with_positions_offsets"
      },

      "content_hash":      { "type": "keyword" },

      "topics":            { "type": "keyword" },
      "page_type":         { "type": "keyword" },

      "tfidf_keywords": {
        "type": "nested",
        "properties": {
          "keyword": { "type": "keyword" },
          "score":   { "type": "float" }
        }
      },

      "seo": {
        "type": "object",
        "properties": {
          "title_length":         { "type": "integer" },
          "meta_desc_length":     { "type": "integer" },
          "h1_count":             { "type": "integer" },
          "has_canonical":        { "type": "boolean" },
          "has_og_tags":          { "type": "boolean" },
          "has_schema_markup":    { "type": "boolean" },
          "internal_link_count":  { "type": "integer" },
          "external_link_count":  { "type": "integer" },
          "word_count":           { "type": "integer" },
          "title_too_long":       { "type": "boolean" },
          "meta_desc_too_long":   { "type": "boolean" },
          "thin_content":         { "type": "boolean" }
        }
      },

      "status_code":       { "type": "integer" },
      "crawl_duration_ms": { "type": "integer" },
      "s3_raw_path":       { "type": "keyword", "index": false }
    }
  }
}
```

**Example queries:**

```json
// "Find all product pages about toasters crawled in June"
GET /page_metadata/_search
{
  "query": {
    "bool": {
      "must": [
        { "match": { "body_text": "toaster" } },
        { "term":  { "page_type": "product" } }
      ],
      "filter": [
        { "range": { "crawled_at": { "gte": "2026-06-01" } } }
      ]
    }
  }
}

// "Pages for amazon.com with keyword density > 2% for 'outdoor gear'"
GET /page_metadata/_search
{
  "query": {
    "bool": {
      "must": [
        { "term": { "domain": "amazon.com" } },
        {
          "nested": {
            "path": "tfidf_keywords",
            "query": {
              "bool": {
                "must": [
                  { "term":  { "tfidf_keywords.keyword": "outdoor gear" } },
                  { "range": { "tfidf_keywords.score":   { "gte": 2.0 } } }
                ]
              }
            }
          }
        }
      ]
    }
  }
}

// "Topic distribution across all crawled domains"
GET /page_metadata/_search
{
  "size": 0,
  "aggs": {
    "topics_per_domain": {
      "terms": { "field": "domain", "size": 100 },
      "aggs": {
        "top_topics": { "terms": { "field": "topics", "size": 10 } }
      }
    }
  }
}
```

---

### 3.3 Redis — Two-Tier Cache + Bloom Filter + robots.txt Cache

Redis is not a durable store. It is a cache and a coordination layer. Three uses:

```
┌─────────────────────────────────────────────────────────────────┐
│  Redis Key Space Design                                          │
│                                                                  │
│  meta:{url_hash}          → JSON(PageMetadata)  TTL=3600s       │
│  content_hash:{url_hash}  → SHA-256(body_text)  TTL=90d        │
│  robots:{domain}          → serialized RobotParser TTL=86400s  │
│  bf:urls_seen             → RedisBloom filter (no TTL)          │
│                             reset at start of each crawl run    │
└─────────────────────────────────────────────────────────────────┘
```

**Memory sizing:**

| Key Space | Count | Avg Size | Total |
|---|---|---|---|
| `meta:*` (hot URL cache) | 10M entries | ~10KB | ~100GB |
| `content_hash:*` | 1B entries | ~50B | ~50GB |
| `robots:*` | ~5M domains | ~2KB | ~10GB |
| `bf:urls_seen` | 1B URLs, 1% FPR | ~1.2GB | ~1.2GB |
| **Total** | | | **~161GB** |

**Deployment**: 3-node Redis Cluster, 64GB RAM each = 192GB capacity.
`content_hash:*` keys can use a separate Redis instance with lower memory if needed
(they have a 90-day TTL and are write-once, read-once per crawl cycle).

```python
# Cache key patterns
META_CACHE_KEY      = "meta:{url_hash}"       # full PageMetadata JSON
CONTENT_HASH_KEY    = "content_hash:{url_hash}"  # SHA-256 of body for change detection
ROBOTS_KEY          = "robots:{domain}"        # pickled RobotFileParser
BLOOM_FILTER_KEY    = "bf:urls_seen"           # RedisBloom, reset per crawl run

# TTLs (in seconds)
META_TTL            = 3600          # 1 hour (aligned with CDN Cache-Control)
CONTENT_HASH_TTL    = 7776000       # 90 days (aligned with Cassandra TTL)
ROBOTS_TTL          = 86400         # 24 hours
# Bloom filter: no TTL — manually reset (FLUSHKEY + BF.RESERVE) at run start
```

---

### 3.4 S3 / GCS — Raw HTML Object Storage

Not a database, but a structured object store. Schema is the path convention:

```
s3://brightedge-crawl-raw/
└── {year}/
    └── {month}/
        └── {domain}/
            └── {url_hash}.html.gz          ← gzipped raw HTML
            └── {url_hash}.meta.json        ← lightweight metadata manifest

# Example:
s3://brightedge-crawl-raw/2026/06/amazon.com/abc123def456.html.gz
s3://brightedge-crawl-raw/2026/06/amazon.com/abc123def456.meta.json
```

**The `.meta.json` manifest** (stored alongside the HTML):
```json
{
  "url_hash":        "abc123def456...",
  "url":             "https://www.amazon.com/...",
  "domain":          "amazon.com",
  "crawled_at":      "2026-06-06T14:23:00Z",
  "status_code":     200,
  "content_type":    "text/html; charset=utf-8",
  "content_length":  182304,
  "crawler_version": "1.2.0"
}
```

**Purpose of the manifest**: Allows S3 Inventory + Athena queries on crawl metadata
without touching the (large) HTML files. "How many pages from amazon.com did we crawl
in June?" is a Parquet scan, not an HTML scan.

**S3 Lifecycle Policy:**
```json
{
  "Rules": [{
    "Status": "Enabled",
    "Filter": { "Prefix": "" },
    "Transitions": [
      { "Days": 30,  "StorageClass": "STANDARD_IA" },
      { "Days": 90,  "StorageClass": "GLACIER" },
      { "Days": 365, "StorageClass": "DEEP_ARCHIVE" }
    ]
  }]
}
```

---

### 3.5 Schema Consistency Strategy (Cross-DB)

The same `PageMetadata` object flows from pipeline → Cassandra → ES → Redis.
Keeping them consistent is critical.

```
Write order (always):
  1. S3 raw HTML (idempotent, key-based)
  2. Cassandra (durable write, upsert by PK)
  3. Redis content_hash (for change detection)
  4. Elasticsearch (write-behind, can be rebuilt)
  5. Redis meta cache (populated on first read, not at write time)

If Cassandra write succeeds but ES write fails:
  → ES is rebuilt from Cassandra via a Kafka replay or a reindex job
  → No data loss (Cassandra is the source of truth)

If Redis evicts a key:
  → Next read misses Redis → falls through to Cassandra → repopulates Redis
  → No data loss (Redis is never the source of truth)
```

**Schema evolution:**
| Store | How to evolve schema |
|---|---|
| Cassandra | `ALTER TABLE ADD column` — online, backward-compatible |
| Elasticsearch | Create new index with alias, reindex from Cassandra, flip alias |
| Redis | Flush + repopulate from Cassandra (cache is ephemeral) |
| S3 | New fields in `.meta.json` are additive; old objects keep old schema |
| Python models | Add field with `field(default_factory=...)` — backward-compatible deserialization |
