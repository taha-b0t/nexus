"""Phase 3 — Processing verification gate (ROADMAP.md).

Spark local-mode batch Bronze->Silver + Structured Streaming Kafka->Bronze,
verified on this VM without Docker and without a live Kafka broker:
- batch runs end-to-end on generated CSVs (JVM-side reads only);
- streaming parse/DLQ/checkpoint logic runs against a file source with
  trigger(once=True);
- the Kafka source plan is built with the real connector package from
  Maven Central (skipped if the jar cannot be resolved); the live-broker
  round-trip stays PENDING-HARDWARE until the Docker stack runs on
  Taha's laptop.

Sandbox constraint (verified 2026-11-16): Python-worker paths
(createDataFrame from Python objects, Python UDFs, toPandas) fail here
with a broken-pipe on the worker socket. Every test below uses JVM-side
ops only (spark.read, built-in functions, collect() to the driver).
"""
import csv
import json
from decimal import Decimal
from pathlib import Path

import pytest

pytest.importorskip("pyspark", reason="pyspark not installed")

from src.processing import bronze_to_silver, stream_kafka_to_bronze
from src.processing.spark_session import get_spark

ROOT = Path(__file__).resolve().parents[1]

# 15 rows: 10 valid (ids 1..10) + 2 late duplicates of ids 1 and 3
# + 3 invalid (qty 0, negative price, missing customer).
BATCH_ROWS = [
    {"order_id": i, "customer_id": 10 + i, "product_id": 100 + i, "city": "Rabat",
     "quantity": 2 if i == 1 else 1, "unit_price": "100.00" if i == 1 else "50.00",
     "status": "paid", "created_at": f"2026-01-0{i}T10:00:00+00:00"}
    for i in range(1, 11)
] + [
    {"order_id": 1, "customer_id": 11, "product_id": 101, "city": "Rabat",
     "quantity": 9, "unit_price": "999.00", "status": "paid",
     "created_at": "2026-01-09T10:00:00+00:00"},  # late duplicate of 1
    {"order_id": 3, "customer_id": 13, "product_id": 103, "city": "Fes",
     "quantity": 9, "unit_price": "999.00", "status": "paid",
     "created_at": "2026-01-09T10:00:00+00:00"},  # late duplicate of 3
    {"order_id": 11, "customer_id": 21, "product_id": 111, "city": "Rabat",
     "quantity": 0, "unit_price": "10.00", "status": "paid",
     "created_at": "2026-01-02T10:00:00+00:00"},  # quantity not positive
    {"order_id": 12, "customer_id": 22, "product_id": 112, "city": "Casablanca",
     "quantity": 1, "unit_price": "-5.00", "status": "paid",
     "created_at": "2026-01-02T10:00:00+00:00"},  # unit price not positive
    {"order_id": 13, "customer_id": "", "product_id": 113, "city": "Agadir",
     "quantity": 1, "unit_price": "10.00", "status": "paid",
     "created_at": "2026-01-02T10:00:00+00:00"},  # missing customer_id
]

STREAM_LINES = [
    {"order_id": 1, "customer_id": 1, "product_id": 1, "city": "Rabat",
     "quantity": 1, "unit_price": 10.0, "event_time": "2026-01-01T10:00:00+00:00"},
    {"order_id": 2, "customer_id": 2, "product_id": 2, "city": "Fes",
     "quantity": 3, "unit_price": 20.0, "event_time": "2026-01-01T10:01:00+00:00"},
    {"order_id": 3, "customer_id": 1, "product_id": 3, "city": "Rabat",
     "quantity": 2, "unit_price": 5.5, "event_time": "2026-01-01T10:02:00+00:00"},
    {"order_id": 4, "customer_id": 3, "product_id": 1, "city": "Tanger",
     "quantity": 1, "unit_price": 99.99, "event_time": "2026-01-01T10:03:00+00:00"},
    "{not json",
    {"customer_id": 9, "product_id": 9, "city": "Rabat", "quantity": 1,
     "unit_price": 1.0, "event_time": "2026-01-01T10:04:00+00:00"},  # no order_id
]


@pytest.fixture(scope="module")
def spark():
    session = get_spark("nexus-tests-phase3")
    yield session
    session.stop()


def _write_orders_csv(path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(BATCH_ROWS[0].keys()))
        w.writeheader()
        w.writerows(BATCH_ROWS)


# ---------------------------------------------------------------- batch
def test_build_bronze_lands_all_rows(spark, tmp_path):
    csv_path = tmp_path / "orders.csv"
    _write_orders_csv(csv_path)
    bronze_path = str(tmp_path / "bronze")
    assert bronze_to_silver.build_bronze(spark, str(csv_path), bronze_path) == 15
    bronze = spark.read.parquet(bronze_path)
    assert bronze.count() == 15
    assert {"_ingested_at", "_source_file"} <= set(bronze.columns)


def test_clean_counts_add_up_and_dedupe_keeps_earliest(spark, tmp_path):
    csv_path = tmp_path / "orders.csv"
    _write_orders_csv(csv_path)
    bronze_path, silver_path, rejected_path = (
        str(tmp_path / "bronze"), str(tmp_path / "silver"), str(tmp_path / "rejected"))
    bronze_to_silver.build_bronze(spark, str(csv_path), bronze_path)
    metrics = bronze_to_silver.run(spark, bronze_path, silver_path, rejected_path)
    assert metrics == {"in": 15, "clean": 10, "rejected": 3, "duplicates_removed": 2}
    assert metrics["in"] == (
        metrics["clean"] + metrics["rejected"] + metrics["duplicates_removed"])

    silver = spark.read.parquet(silver_path)
    assert silver.count() == 10
    # dedupe kept the EARLIEST created_at: order 1 has qty 2, not the dup's 9
    row = silver.filter("order_id = 1").select("quantity", "unit_price", "amount").collect()
    assert len(row) == 1
    assert row[0]["quantity"] == 2
    assert row[0]["unit_price"] == Decimal("100.00")
    assert row[0]["amount"] == Decimal("200.00")  # quantity * unit_price
    assert silver.filter("order_id = 3").select("quantity").collect()[0]["quantity"] == 1


def test_rejected_rows_carry_reasons(spark, tmp_path):
    csv_path = tmp_path / "orders.csv"
    _write_orders_csv(csv_path)
    bronze_path, silver_path, rejected_path = (
        str(tmp_path / "bronze"), str(tmp_path / "silver"), str(tmp_path / "rejected"))
    bronze_to_silver.build_bronze(spark, str(csv_path), bronze_path)
    bronze_to_silver.run(spark, bronze_path, silver_path, rejected_path)
    rejected = spark.read.parquet(rejected_path)
    reasons = {r["order_id"]: r["rejection_reason"]
               for r in rejected.select("order_id", "rejection_reason").collect()}
    assert reasons[11] == "quantity_not_positive"
    assert reasons[12] == "unit_price_not_positive"
    assert reasons[13] == "missing_customer_id"


def test_silver_schema_is_typed(spark, tmp_path):
    csv_path = tmp_path / "orders.csv"
    _write_orders_csv(csv_path)
    bronze_path, silver_path, rejected_path = (
        str(tmp_path / "bronze"), str(tmp_path / "silver"), str(tmp_path / "rejected"))
    bronze_to_silver.build_bronze(spark, str(csv_path), bronze_path)
    bronze_to_silver.run(spark, bronze_path, silver_path, rejected_path)
    schema = {f.name: f.dataType for f in spark.read.parquet(silver_path).schema}
    assert (schema["unit_price"].precision, schema["unit_price"].scale) == (10, 2)
    assert (schema["amount"].precision, schema["amount"].scale) == (12, 2)
    assert type(schema["created_at"]).__name__ == "TimestampType"
    assert set(bronze_to_silver.SILVER_COLUMNS) <= set(schema)


# ---------------------------------------------------------------- streaming
def test_streaming_file_source_splits_valid_and_dlq(spark, tmp_path):
    src_dir = tmp_path / "in"
    src_dir.mkdir()
    with open(src_dir / "events.jsonl", "w", encoding="utf-8") as f:
        for line in STREAM_LINES:
            f.write(line if isinstance(line, str) else json.dumps(line))
            f.write("\n")
    raw = spark.readStream.format("text").load(str(src_dir))
    assert raw.isStreaming
    valid, dlq = stream_kafka_to_bronze.parse_stream(raw)
    assert valid.isStreaming and dlq.isStreaming

    bronze_path, dlq_path, ckpt = (
        str(tmp_path / "bronze"), str(tmp_path / "dlq"), str(tmp_path / "ckpt"))
    q1 = stream_kafka_to_bronze.build_bronze_query(
        valid, bronze_path, ckpt, trigger_once=True)
    q2 = stream_kafka_to_bronze.build_dlq_query(
        dlq, dlq_path, ckpt, trigger_once=True)
    q1.awaitTermination(timeout=120)
    q2.awaitTermination(timeout=120)
    assert not q1.isActive and not q2.isActive

    bronze = spark.read.parquet(bronze_path)
    assert bronze.count() == 4
    assert sorted(r["order_id"] for r in
                  bronze.select("order_id").collect()) == [1, 2, 3, 4]
    assert type(bronze.schema["event_time"].dataType).__name__ == "TimestampType"
    dlq_df = spark.read.parquet(dlq_path)
    assert dlq_df.count() == 2
    reasons = sorted(r["rejection_reason"] for r in
                     dlq_df.select("rejection_reason").collect())
    assert reasons == ["missing_order_id", "unparseable_json"]
    # checkpoints were actually written (restart resumes from them)
    assert (Path(ckpt) / "bronze" / "offsets").exists()
    assert (Path(ckpt) / "dlq" / "offsets").exists()


def test_kafka_source_plan_builds_with_connector_package():
    """The Kafka readStream plan builds (no broker needed for load()).

    Runs in a fresh interpreter: SparkSession.getOrCreate() reuses the
    module-scoped session (no kafka package), so the connector session
    must be built in its own process. Skipped only when the connector jar
    cannot be resolved from Maven Central.
    """
    import subprocess
    import sys

    snippet = (
        "from src.processing import stream_kafka_to_bronze\n"
        "from src.processing.kafka_jars import ensure_kafka_jars\n"
        "from src.processing.spark_session import get_spark\n"
        "spark = get_spark('nexus-tests-kafka-plan',\n"
        "                  extra_jars=ensure_kafka_jars())\n"
        "df = stream_kafka_to_bronze.kafka_stream(spark, 'localhost:9092', 'nexus.orders')\n"
        "cols = {'key', 'value', 'topic', 'partition', 'offset', 'timestamp', 'timestampType'}\n"
        "assert cols <= set(df.columns), sorted(df.columns)\n"
        "print('KAFKA PLAN OK', sorted(df.columns))\n"
        "spark.stop()\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", snippet], cwd=str(ROOT),
        capture_output=True, text=True, timeout=300)
    if proc.returncode == 0:
        assert "KAFKA PLAN OK" in proc.stdout
        return
    stderr = proc.stderr
    if ("Failed to find data source: kafka" in stderr
            or "unresolved dependency" in stderr.lower()
            or "Could not resolve" in stderr
            or "could not download" in stderr):
        pytest.skip(f"kafka connector jar not resolvable: {stderr[-500:]}")
    pytest.fail(f"kafka plan build failed:\n{stderr[-2000:]}")
