# Part 3 — Proof of Concept Plan, Estimates & Release Strategy

## PoC Goals

1. Does the crawler correctly extract metadata + topics for the test URLs?
2. Does the pipeline hold at moderate scale (100K URLs, no data loss)?
3. Is API p99 <100ms under read load?

**Out of scope for PoC:** full 1B-URL crawl, GPU NLP, multi-region.

## Phases & Estimates

| Phase | Scope | Duration | Exit Criteria |
|---|---|---|---|
| 1 | Crawler + extraction | Week 1 | All 3 test URLs return correct JSON |
| 2 | NLP pipeline | Week 1–2 | Topics, page_type, keyword_density populated |
| 3 | API + cache | Week 2 | 1,000 req/sec, p99 <50ms (cache hit) |
| 4 | Kafka + Cassandra + deploy (AWS/GCP) | Week 3 | 100K URLs stored, public endpoint live |
| 5 | Monitoring, hardening, demo | Week 4 | 4 Grafana dashboards, all SLO alerts wired |

**Total:** 4 weeks. Buffer: +3 days for JS rendering, +2 days for infra surprises.

## Blockers

### Known, Trivial
| Blocker | Mitigation |
|---|---|
| Amazon blocks bots | Use REI/CNN for live demo; note Playwright path |
| KeyBERT ~90MB download | Pre-download in Docker build |
| SSL errors on some domains | `ssl=False` for demo |

### Known, Non-Trivial
| Blocker | Risk | Mitigation | ETA |
|---|---|---|---|
| JS-rendered pages (SPAs) | Empty body on Amazon | Playwright headless fallback | +3 days |
| Cassandra cluster ops | Complex for first-time setup | Use AWS Keyspaces (managed) for PoC | Eliminates blocker |
| Kafka at scale | Default configs not prod-ready | Use AWS MSK (managed) for PoC | Eliminates blocker |
| KeyBERT CPU latency (~400ms) | Too slow for 385 URLs/sec | GPU deferred to post-PoC | Not a PoC blocker |

### Unknown / High-Risk
| Blocker | Signal to Watch |
|---|---|
| ES performance at 1B docs | Load test with 10M docs in Phase 4, extrapolate |
| Cassandra hot partition for popular domains | Monitor node CPU during 100K test |

## PoC Pass Criteria

| Category | Metric | Threshold |
|---|---|---|
| Quality | Metadata accuracy (100 URLs, manual review) | >90% correct title/desc/h1 |
| Quality | Topic relevance (50 URLs, top-5 topics) | >80% relevant |
| Quality | Page type accuracy (100 labeled URLs) | >85% correct |
| Performance | Crawl throughput | >100 URLs/sec sustained |
| Performance | API p99 (cache hit) | <50ms |
| Performance | API p99 (Cassandra read) | <20ms |
| Reliability | Crawl success rate | >97% |
| Reliability | Data loss on worker restart | 0 |

## Release Checklist

- [ ] Docker images pushed to ECR/GCR
- [ ] K8s resource limits + liveness probes set
- [ ] Secrets in AWS Secrets Manager (not env vars)
- [ ] Cassandra replication factor = 3
- [ ] Redis sentinel/cluster mode (no SPOF)
- [ ] Kafka replication factor = 3, `min.insync.replicas = 2`
- [ ] 4 Grafana dashboards live + SLO alerts firing
- [ ] Runbook written: scale up crawlers, reprocess failed URLs
- [ ] Rollback plan: feature flag to disable NLP without redeployment
- [ ] Load test: 10K req/sec, p99 <100ms

## Post-PoC Optimizations

1. GPU NLP workers (10× throughput, 5× cost reduction per URL)
2. Playwright fallback for JS-rendered pages (~15% of web)
3. Multi-region deployment (US + EU)
4. Incremental crawling via HTTP `If-Modified-Since`
