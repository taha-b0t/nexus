# LEARNING — everything NEXUS uses, in the order to learn it

Written so the author (and any reviewer) can see *why* each tool exists here, the exact concepts NEXUS relies on, and a hands-on check for each. No tool is in the stack for decoration.

## 0. Foundations first: Python + SQL + Git + Linux/Docker basics
- **Python**: dataclasses, generators (streaming rows without loading RAM), `pathlib`, venv, pytest. Used in: generators, producers, API, ML.
- **SQL**: JOIN/GROUP BY, CTEs, window functions, EXPLAIN. Used in: Postgres, dbt, Trino — same language, three engines.
- **Git/GitHub Actions**: branches, PRs, CI on push. *Check: CI in `.github/workflows/ci.yml` runs pytest.*

## 1. PostgreSQL — operational database (Phase 1)
Concepts: schemas, primary/foreign keys, indexes, transactions/ACID, WAL (write-ahead log — this is what makes CDC possible), roles & least privilege.
In NEXUS: source of truth for orders/payments/inventory; Debezium reads its WAL; Trino federates it.
*Check: `infra/postgres/init.sql` creates the schema + read-only `nexus_readonly` role the AI layer uses.*

## 2. Docker & Docker Compose — reproducibility (Phase 1)
Concepts: image vs container, volumes (data survives containers), networks (services find each other by name), profiles (start only what a phase needs), healthchecks.
*Check: `docker-compose.yml` — `docker compose --profile full up` vs the default lightweight profile. On a small laptop, never run the full profile blindly: Kafka+Spark+Trino+Airflow together need ~8–10 GB RAM.*

## 3. Apache Kafka — event streaming (Phase 2)
Concepts: topic = ordered log, partition = parallelism + ordering per key, consumer group, offset = position, retention, schema evolution. Kafka is a *log*, not a queue: events stay and can be replayed.
In NEXUS: backbone between sources and Spark; key events by `order_id`/`customer_id` so a customer's events stay ordered.
Pitfalls: wrong partitioning key breaks ordering; no schema discipline breaks consumers.
*Check: produce with `src/ingestion/producers.py`, consume and print offsets.*

## 4. Debezium + CDC — Change Data Capture (Phase 2)
Concepts: CDC = stream *changes* (insert/update/delete) instead of re-querying tables. Debezium reads the Postgres WAL (logical decoding, replication slot). Snapshot + streaming modes. Delete handling / tombstones.
In NEXUS: Postgres → `cdc.public.*` Kafka topics, so analytics sees changes in near-real-time without hammering the operational DB.
*Check: update a row in Postgres, watch the change event appear on the topic (`infra/debezium/register-connector.json`).*

## 5. Apache Spark — distributed processing (Phase 3)
Concepts: DataFrame API, lazy evaluation & Catalyst optimiser, shuffle (expensive — minimise it), partitioning, cache only when reused.
**Batch**: Bronze→Silver cleaning. **Structured Streaming**: Kafka→Bronze micro-batches, checkpointing (stores offsets+state so a crashed job resumes), watermarking for late events, output modes.
Pitfalls: small files, data skew, collecting to driver.
*Check: `src/processing/` jobs run in local mode on synthetic data before any cluster talk.*

## 6. MinIO + Parquet + Apache Iceberg — the lakehouse (Phase 4)
- **Object storage (MinIO)**: S3 API, buckets, no POSIX semantics — files are objects. Cheap, decoupled from compute.
- **Parquet**: columnar file format — read only the columns you need, compression + predicate pushdown via row-group statistics.
- **Iceberg**: table format *on top of* Parquet objects — adds ACID transactions, schema evolution, partition evolution, time travel, hidden partitioning, merge-into (upserts for CDC). Solves: "a folder of files is not a table."
Catalog: Iceberg needs a catalog (here: JDBC catalog on Postgres / REST) to track current snapshots.
*Check: `src/lakehouse/medallion.py` — write Bronze, evolve schema, read an older snapshot (time travel).*

## 7. Trino — distributed SQL query engine (Phase 5)
Concepts: coordinator/workers, connectors (Iceberg, Postgres federated in one query), cost-based optimisation, EXPLAIN ANALYZE.
In NEXUS: the single SQL door to Gold for dbt verification, API, and NL analytics.

## 8. dbt — analytical transformations (Phase 5)
Concepts: models = SELECT statements materialised as tables/views, refs (DAG of models), sources, tests (unique/not_null/relationships/custom), docs, staging → intermediate → marts layering.
Rule: dbt transforms *inside* the warehouse/lakehouse (ELT), it does not extract/load.
*Check: `dbt/` — `dbt build` runs models + tests together.*

## 9. Apache Airflow — orchestration (Phase 6)
Concepts: DAG, task, operator, schedule, retries/backoff, sensors, idempotent tasks (re-running a date must be safe), backfill.
Rule: Airflow *orchestrates* — it triggers Spark/dbt, it does not process data itself.
*Check: `airflow/dags/nexus_daily.py`.*

## 10. Great Expectations — data quality (Phase 6)
Concepts: expectation suites (not-null, unique, in-set, freshness, volume), checkpoints, validation as a *gate*: Silver data that fails does not reach Gold.
In NEXUS the quality score in BENCHMARKS.md comes from these suites.

## 11. FastAPI — serving layer (Phase 7)
Concepts: typed endpoints (Pydantic), dependency injection for DB sessions, read-only queries, pagination + limits. Docs auto-generated at `/docs`.

## 12. Machine learning for anomaly detection — scikit-learn (Phase 7)
Concepts: IsolationForest (unsupervised — no labels needed), feature engineering (amount vs customer average, hour, velocity), contamination rate, precision problem: anomalies are alerts for review, not proof of fraud.
*Check: `src/analytics/anomaly.py` runs on generated orders and flags injected outliers.*

## 13. LLM-powered NL analytics — safely (Phase 7)
Concepts: prompt with *approved schema only*, SQL generation, then a deterministic validator (parse SQL, allow SELECT only, allow-list Gold tables, forbid `*` on sensitive tables, inject LIMIT, statement timeout), execute as read-only role, explain results.
Never: let the model execute raw, give it write credentials, or skip the validator because the demo works.

## 14. Prometheus + Grafana — observability (Phase 8)
Concepts: metrics (counters/gauges/histograms), scraping, dashboards for throughput/latency/freshness/failures. Logs tell you *what happened*; metrics tell you *it is happening*.

## 15. Apache Flink — optional (post-v1)
True streaming (event-time, stateful, lower latency than micro-batch). Learn only after Spark streaming is understood: same questions (state, time, exactly-once), different answers.

## Learning order (do not invert)
Postgres/SQL → Docker → Kafka → Debezium → Spark → Iceberg/MinIO/Parquet → Trino → dbt → Airflow → Great Expectations → FastAPI → scikit-learn → NL-analytics safety → Prometheus/Grafana → (Flink).
Each step unlocks the next; skipping to Flink/AI first produces demos that collapse on data problems.
