# Roadmap — 8 phases

Each phase ships a runnable increment + tests + a benchmark entry. Nothing is marked done without a measured result.

1. **Foundation** — Postgres schema, synthetic generator (customers/products/orders/payments/inventory/deliveries for 10 Moroccan cities), Docker Compose, pytest. ✅
2. **Streaming** — Kafka (KRaft), Avro/JSON schemas in `infra/kafka/schemas/`, Debezium connector for Postgres CDC, Python producers.
3. **Processing** — Spark batch Bronze→Silver, Structured Streaming Kafka→Bronze with checkpointing + DLQ.
4. **Lakehouse** — MinIO buckets `bronze/silver/gold`, Iceberg catalog (REST/JDBC on Postgres), Parquet, compaction job.
5. **Analytics engineering** — Trino, dbt staging/intermediate/marts, dimensional model, dbt tests.
6. **Orchestration & quality** — Airflow DAGs, Great Expectations suites gating promotion, freshness checks.
7. **Intelligence** — IsolationForest anomaly detection on transactions, FastAPI, NL→SQL with validator.
8. **Production readiness** — Prometheus/Grafana dashboards, GitHub Actions CI, failure-injection + recovery-time measurement, benchmark report, full docs.
