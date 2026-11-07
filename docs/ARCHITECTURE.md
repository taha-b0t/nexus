# NEXUS — Architecture

## Data flow
1. **Operational writes** land in PostgreSQL (customers, products, orders, payments, inventory, deliveries).
2. **Debezium** tails the PostgreSQL WAL and publishes row changes to Kafka topics (`cdc.public.orders`, …).
3. **Synthetic generators / REST ingestion** publish behavioural events (page views, cart, checkout) to Kafka directly.
4. **Spark Structured Streaming** consumes topics, writes Bronze Iceberg tables on MinIO (Parquet files), with checkpointing for exactly-once-ish, idempotent recovery.
5. **Spark batch / dbt** builds Silver (clean, dedupe on natural keys, schema enforcement) and Gold (star schema).
6. **Trino** queries Iceberg Gold tables federated with PostgreSQL.
7. **FastAPI** serves aggregates; the NL analytics layer generates validated, read-only SQL only against Gold.
8. **Airflow** schedules batch, compaction, quality checks; **Great Expectations** gates Silver/Gold promotion.
9. **Prometheus/Grafana** scrape Kafka, Spark, Airflow and API metrics: throughput, end-to-end latency, freshness, failures.

## Fault tolerance
Checkpointing (Spark), retries with backoff (Airflow), idempotent writes (merge-into on keys in Iceberg), dead-letter topic `dlq.events` for poison records, replay from Bronze (raw is never mutated).

## Performance levers (measured, see BENCHMARKS.md)
Partitioning by `event_date` / city, predicate pushdown into Iceberg/Parquet, file compaction (small-files problem), Z-ordering on product/customer keys where it pays, Trino query planning.

## Security
Least-privilege roles: ingestion writer, transformation writer, API reader (Gold only, read-only), AI role (Gold only, SELECT only, statement timeout, row cap).
