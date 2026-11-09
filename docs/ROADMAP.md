# Roadmap — 2-month build plan (7 Nov 2026 → 7 Jan 2027)

One phase per week, every Monday a new phase starts. **No phase is marked done
without verification** (see `docs/VERIFICATION.md`): tests must pass, the commit
must be read back live from GitHub, and the verification log must be updated in
the same commit.

| Week | Dates (Africa/Casablanca) | Phase | Deliverable | Verification gate |
|---|---|---|---|---|
| 0 | Sat 7 Nov 2026 | 1 — Foundation ✅ | Postgres schema, synthetic generator, Docker Compose, docs | 6/6 tests, commit `56524a0` read back live |
| 1 | Mon 9 – Sun 15 Nov | 2 — Streaming ✅ | Kafka (KRaft) compose profile, topic + schema definitions, Python producers/consumers, Debezium connector config validated | Producer/consumer round-trip test in lightweight mode; schemas parse; CDC config validated against Postgres init schema |
| 2 | Mon 16 – Sun 22 Nov | 3 — Processing | Spark batch Bronze→Silver (working local-mode job), Structured Streaming job with checkpointing + DLQ design | Spark local run on synthetic data: row counts in = cleaned + rejected; dedupe proven by test |
| 3 | Mon 23 – Sun 29 Nov | 4 — Lakehouse | Parquet Bronze/Silver on local/S3 paths, Iceberg table definitions, Medallion promotion logic, compaction notes | Round-trip write/read test; schema-evolution test; partition layout documented |
| 4 | Mon 30 Nov – Sun 6 Dec | 5 — Analytics engineering | dbt project completed (staging/intermediate/marts), dimensional model, Trino config + canonical queries | dbt model SQL linted/tested against local DuckDB/Postgres stand-in where Trino unavailable; dbt tests defined and passing in CI mode |
| 5 | Mon 7 – Sun 13 Dec | 6 — Orchestration & quality | Airflow DAGs (daily + backfill), Great Expectations suites for Silver/Gold gates | DAG imports parse cleanly; GE suites run against generated Silver sample and produce a real pass rate |
| 6 | Mon 14 – Sun 20 Dec | 7 — Intelligence | Anomaly detection (IsolationForest + z-score) on generated data, FastAPI endpoints, NL→SQL validator hardened | Detector flags injected anomalies (precision/recall noted); validator test-suite blocks writes/unknown tables; API tested with TestClient |
| 7 | Mon 21 – Sun 27 Dec | 8 — Production readiness | Prometheus/Grafana dashboards as code, CI green, failure/recovery runbook, README final pass | CI passes on GitHub; runbook steps each executed or explicitly marked unexecuted-with-reason |
| 8 | Mon 28 Dec – Thu 7 Jan 2027 | Final verification | Full-repo audit, BENCHMARKS updated with every measured number, demo script, v1.0.0 tag | Every claim in README traced to a log/test; anything unverifiable on current hardware marked honestly; final report to Taha |

## Environment honesty rule
This build runs on a VM with no Docker and limited RAM. Each phase is verified
in the strongest mode the environment allows (unit tests, local-mode engines,
config validation, live GitHub read-back). Anything that genuinely needs the
full Docker stack is marked `PENDING-HARDWARE` in VERIFICATION.md — never
claimed as run. The full-stack run is scheduled for Taha's laptop (16 GB RAM).
