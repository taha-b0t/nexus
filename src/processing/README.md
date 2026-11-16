# Processing (Phase 3 ✅)

## Batch: Bronze -> Silver

`bronze_to_silver.py` — Spark batch job, local mode.

```
python -m src.processing.bronze_to_silver \
    --csv data/synthetic/orders.csv \
    --bronze data/bronze/orders --silver data/silver/orders \
    --rejected data/silver/orders_rejected
```

- `build_bronze`: lands a raw orders CSV as typed Bronze Parquet, plus
  `_ingested_at` and `_source_file` metadata. Bronze is never mutated.
- `clean_orders`: dedupes on the natural key `order_id` (earliest
  `created_at` wins), validates (ids present, `quantity > 0`,
  `unit_price > 0`, city present), normalises types (`unit_price`
  decimal(10,2), `created_at` timestamp) and derives
  `amount = quantity * unit_price`.
- Quality gate (checked by the job itself, exit non-zero on violation):
  `in == clean + rejected + duplicates_removed`. Rejected rows are kept
  in the quarantine dataset with a `rejection_reason` per failed rule.

## Streaming: Kafka -> Bronze

`stream_kafka_to_bronze.py` — Structured Streaming job.

```
python -m src.processing.stream_kafka_to_bronze \
    --bootstrap kafka:9092 --topic nexus.orders \
    --bronze data/bronze/orders_stream --dlq data/dlq/orders \
    --checkpoint data/checkpoints/kafka_bronze
```

- Kafka source (`spark-sql-kafka-0-10_2.12:3.5.3`, resolved from Maven
  Central), `startingOffsets=earliest`, append mode to Bronze Parquet.
- Checkpointing under `data/checkpoints/kafka_bronze/{bronze,dlq}`:
  a restart resumes from the last committed offset.
- 10-minute watermark on `event_time`; events later than the watermark
  are dropped by Spark (documented trade-off, not silent loss).
- DLQ: unparseable JSON or events without `order_id` go to the DLQ
  Parquet dataset with the raw value, the reason, and Kafka offsets.
  In the full stack the same records also go to the `dlq.events` topic.
  Replay source is the DLQ dataset: fix the producer, replay the files.
- Business-rule validation stays in Bronze -> Silver; Bronze keeps raw.

## Running on this VM (sandbox limits, verified 2026-11-16)

- No Docker, no live Kafka: the batch job runs end-to-end in Spark
  local mode; the streaming parse/DLQ/checkpoint logic is verified with
  a file source (`tests/test_processing.py`); the Kafka source plan is
  verified to build with the real connector jar. The live-broker
  round-trip is `PENDING-HARDWARE` (Taha's laptop, full Docker stack).
- Python-worker paths are broken here (forked worker socket dies with
  "Broken pipe"): use JVM-side DataFrame ops only — no `createDataFrame`
  from Python objects, no Python UDFs, no `toPandas`. `collect()` back
  to the driver works.
- Java: `JAVA_HOME` env var wins; otherwise `src/processing/spark_session.py`
  falls back to `~/jdk-11.0.32.1+1`. Same for `SPARK_HOME`
  (`~/spark-3.5.3-bin-hadoop3`). Driver is pinned to `127.0.0.1` because
  the sandbox hostname resolves to the gateway IP and breaks task sockets.
