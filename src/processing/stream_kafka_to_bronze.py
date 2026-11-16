"""Structured Streaming Kafka -> Bronze (NEXUS Phase 3).

Pipeline: Kafka topic `nexus.orders` -> parse JSON value -> Bronze Parquet
(raw events, append mode) with checkpointing; unparseable or key-less
records go to a DLQ Parquet dataset instead of killing the query.

Exactly-once-ish recovery: `checkpointLocation` + `outputMode=append` make
a restart resume from the last committed Kafka offset; replays collapse
later in Bronze -> Silver dedupe (Phase 3 batch job, natural key order_id).

Late events: 10-minute watermark on `event_time`. Events later than the
watermark are dropped by Spark by design; the watermark is a documented
trade-off, not silent loss (see README).

DLQ design:
- DLQ sink is Parquet at `<dlq_path>` (query-local, works without Kafka).
- In the full Docker stack the same poison records are ALSO published to
  the `dlq.events` Kafka topic (see docs/ARCHITECTURE.md); the Parquet DLQ
  is the replay source: fix the producer, replay the DLQ files through
  Bronze -> Silver. A DLQ record keeps the raw value, the failure reason,
  and the Kafka offsets (topic/partition/offset) when available.
- Business-rule validation (quantity > 0, prices, ...) does NOT happen
  here: Bronze keeps raw events; Bronze -> Silver is the quality gate.

The Kafka source needs the connector package
`org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3` (Maven Central).
`get_spark(..., extra_packages=[...])` adds it; without it the Kafka
readStream fails at plan time, which the tests assert separately from the
parse/DLQ logic (verified here with a file source, no broker needed).

Usage (full stack, needs Kafka up):
    python -m src.processing.stream_kafka_to_bronze \
        --bootstrap kafka:9092 --topic nexus.orders \
        --bronze data/bronze/orders_stream --dlq data/dlq/orders \
        --checkpoint data/checkpoints/kafka_bronze
"""
import argparse

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

KAFKA_CONNECTOR_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
WATERMARK = "10 minutes"

ORDER_EVENT_SCHEMA = StructType([
    StructField("order_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("product_id", LongType(), True),
    StructField("city", StringType(), True),
    StructField("quantity", LongType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("event_time", StringType(), True),
])

BRONZE_COLUMNS = [
    "order_id", "customer_id", "product_id", "city", "quantity",
    "unit_price", "event_time",
]


def kafka_stream(spark, bootstrap_servers, topic):
    """Streaming DataFrame over a Kafka topic (binary key/value columns)."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )


def parse_stream(raw_df):
    """Split a raw stream (column `value`, binary or string) into
    (valid_df, dlq_df). valid_df carries typed event columns + event_time
    as timestamp; dlq_df keeps the raw value, reason, and Kafka offsets
    when the source provides them."""
    parsed = raw_df.withColumn(
        "_raw", F.col("value").cast(StringType())
    ).withColumn(
        "_event", F.from_json(F.col("_raw"), ORDER_EVENT_SCHEMA)
    )
    # from_json runs in PERMISSIVE mode: malformed JSON yields an all-null
    # struct, not a null struct. So "unparseable" == every event field null;
    # "missing_order_id" == order_id null but some other field present.
    all_null = None
    for field in ORDER_EVENT_SCHEMA.fieldNames():
        cond = F.col("_event")[field].isNull()
        all_null = cond if all_null is None else (all_null & cond)
    is_unparseable = F.col("_event").isNull() | all_null
    is_valid = ~is_unparseable & F.col("_event.order_id").isNotNull()
    reason = (
        F.when(is_unparseable, F.lit("unparseable_json"))
        .when(F.col("_event.order_id").isNull(), F.lit("missing_order_id"))
        .otherwise(F.lit("unknown"))
    )
    valid = (
        parsed.filter(is_valid)
        .select(
            F.col("_event.order_id").alias("order_id"),
            F.col("_event.customer_id").alias("customer_id"),
            F.col("_event.product_id").alias("product_id"),
            F.col("_event.city").alias("city"),
            F.col("_event.quantity").alias("quantity"),
            F.col("_event.unit_price").alias("unit_price"),
            F.to_timestamp(F.col("_event.event_time")).alias("event_time"),
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )
    dlq_cols = [F.col("_raw").alias("raw_value"), reason.alias("rejection_reason"),
                F.current_timestamp().alias("_dlq_at")]
    for kafka_col in ("topic", "partition", "offset", "timestamp"):
        if kafka_col in raw_df.columns:
            dlq_cols.append(F.col(kafka_col))
    dlq = parsed.filter(~is_valid).select(*dlq_cols)
    return valid, dlq


def build_bronze_query(valid_df, bronze_path, checkpoint_path, trigger_once=False):
    """Append-mode Bronze sink with a 10-minute watermark on event_time."""
    writer = (
        valid_df.withWatermark("event_time", WATERMARK)
        .writeStream.format("parquet")
        .outputMode("append")
        .option("path", bronze_path)
        .option("checkpointLocation", f"{checkpoint_path}/bronze")
    )
    if trigger_once:
        writer = writer.trigger(once=True)
    return writer.start()


def build_dlq_query(dlq_df, dlq_path, checkpoint_path, trigger_once=False):
    """Append-mode DLQ sink (no watermark: DLQ rows have no event_time)."""
    writer = (
        dlq_df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", dlq_path)
        .option("checkpointLocation", f"{checkpoint_path}/dlq")
    )
    if trigger_once:
        writer = writer.trigger(once=True)
    return writer.start()


def run_kafka_to_bronze(spark, bootstrap_servers, topic, bronze_path,
                        dlq_path, checkpoint_path):
    """Wire Kafka source -> Bronze + DLQ and start both queries (blocking)."""
    raw = kafka_stream(spark, bootstrap_servers, topic)
    valid, dlq = parse_stream(raw)
    q_bronze = build_bronze_query(valid, bronze_path, checkpoint_path)
    q_dlq = build_dlq_query(dlq, dlq_path, checkpoint_path)
    q_bronze.awaitTermination()
    q_dlq.awaitTermination()
    return q_bronze, q_dlq


def main():
    ap = argparse.ArgumentParser(description="NEXUS Kafka -> Bronze streaming job")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="nexus.orders")
    ap.add_argument("--bronze", required=True, help="Bronze Parquet path")
    ap.add_argument("--dlq", required=True, help="DLQ Parquet path")
    ap.add_argument("--checkpoint", required=True, help="checkpoint base path")
    args = ap.parse_args()

    from src.processing.kafka_jars import ensure_kafka_jars
    from src.processing.spark_session import get_spark

    try:
        jars = ensure_kafka_jars()
        spark = get_spark("nexus-kafka-to-bronze", extra_jars=jars)
    except RuntimeError as exc:
        print(f"local kafka jars unavailable ({exc}); falling back to Ivy")
        spark = get_spark("nexus-kafka-to-bronze",
                          extra_packages=[KAFKA_CONNECTOR_PACKAGE])
    try:
        run_kafka_to_bronze(spark, args.bootstrap, args.topic,
                            args.bronze, args.dlq, args.checkpoint)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
