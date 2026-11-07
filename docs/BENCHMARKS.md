# Benchmarks — measured, not assumed

| Metric | Purpose | How measured | Result |
|---|---|---|---|
| Ingestion throughput | events/sec Kafka→Bronze | producer timestamps vs Bronze commit | _to be measured in Phase 2/3_ |
| End-to-end latency | event → queryable in Gold | event_time vs Gold availability | _to be measured_ |
| Batch duration | Silver/Gold build time | Spark job duration | _to be measured_ |
| Query time | Trino on Gold | EXPLAIN ANALYZE on 5 canonical queries | _to be measured_ |
| Data quality score | % expectations passing | Great Expectations suite | _to be measured_ |
| Recovery time | kill a job → recovered | failure-injection runbook | _to be measured_ |

Rule: a number enters this table only from a run logged in `benchmarks/` with dataset size, hardware, and config.
