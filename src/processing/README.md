# Processing (Phase 3)

- `bronze_to_silver.py` — Spark batch: dedupe on natural keys, normalise types, flag quality issues.
- `stream_kafka_to_bronze.py` — Structured Streaming: Kafka -> Iceberg Bronze, checkpointing in `data/checkpoints/`, watermark 10 min for late events, poison records to `dlq.events`.

Run Spark in local mode first. Do not add cluster config until local benchmarks exist (see docs/BENCHMARKS.md).
