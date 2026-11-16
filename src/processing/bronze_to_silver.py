"""Spark batch Bronze -> Silver (NEXUS Phase 3).

Bronze = raw orders as landed (typed Parquet, plus ingestion metadata).
Silver = deduped, type-normalised, validated orders + a derived amount.

Quality contract (the Phase 3 verification gate):
    rows_in == rows_clean + rows_rejected + rows_duplicates_removed
Every input row lands in exactly one of the three outputs, so the gate is
checkable by counting. Rejected rows are kept in a quarantine Parquet
dataset with a `rejection_reason` column (never silently dropped).

Dedupe rule: one row per natural key `order_id`, keeping the earliest
`created_at` (first-write-wins; replays of the same order collapse).

Validation rules (a row failing any rule is rejected):
- order_id / customer_id / product_id present
- quantity > 0
- unit_price > 0
- city present

All logic is JVM-side DataFrame ops only (no Python UDFs, no
createDataFrame): the sandbox Python worker is broken, see
src/processing/spark_session.py.

Usage:
    python -m src.processing.bronze_to_silver \
        --csv data/synthetic/orders.csv \
        --bronze data/bronze/orders --silver data/silver/orders \
        --rejected data/silver/orders_rejected
    # or, with Bronze already landed:
    python -m src.processing.bronze_to_silver \
        --bronze data/bronze/orders --silver data/silver/orders \
        --rejected data/silver/orders_rejected
"""
import argparse

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

ORDER_BRONZE_SCHEMA = StructType([
    StructField("order_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("product_id", LongType(), True),
    StructField("city", StringType(), True),
    StructField("quantity", LongType(), True),
    StructField("unit_price", DecimalType(10, 2), True),
    StructField("status", StringType(), True),
    StructField("created_at", TimestampType(), True),
])

SILVER_COLUMNS = [
    "order_id", "customer_id", "product_id", "city", "quantity",
    "unit_price", "amount", "status", "created_at",
]


def build_bronze(spark, csv_path, bronze_path):
    """Land a raw orders CSV as typed Bronze Parquet + ingestion metadata."""
    df = (
        spark.read.option("header", "true")
        .schema(ORDER_BRONZE_SCHEMA)
        .csv(csv_path)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )
    df.write.mode("overwrite").parquet(bronze_path)
    return df.count()


def clean_orders(bronze_df):
    """Split Bronze orders into (clean, rejected, metrics).

    Returns (clean_df, rejected_df, metrics dict). Metrics:
    in / clean / rejected / duplicates_removed, with the invariant
    in == clean + rejected + duplicates_removed.
    """
    n_in = bronze_df.count()

    # --- dedupe on the natural key, earliest created_at wins ---------------
    w = Window.partitionBy("order_id").orderBy(F.col("created_at").asc_nulls_last())
    ranked = bronze_df.withColumn("_rn", F.row_number().over(w))
    deduped = ranked.filter(F.col("_rn") == 1).drop("_rn")
    n_duplicates = n_in - deduped.count()

    # --- validation: name every failed rule on the rejected row ------------
    checks = {
        "missing_order_id": F.col("order_id").isNull(),
        "missing_customer_id": F.col("customer_id").isNull(),
        "missing_product_id": F.col("product_id").isNull(),
        "missing_city": F.col("city").isNull() | (F.col("city") == ""),
        "quantity_not_positive": F.col("quantity").isNull() | (F.col("quantity") <= 0),
        "unit_price_not_positive": F.col("unit_price").isNull() | (F.col("unit_price") <= 0),
    }
    invalid = None
    for cond in checks.values():
        invalid = cond if invalid is None else (invalid | cond)
    reason = F.concat_ws(
        ";",
        *[F.when(c, F.lit(name)) for name, c in checks.items()],
    )
    rejected = deduped.filter(invalid).withColumn("rejection_reason", reason)
    valid = deduped.filter(~invalid)

    # --- normalise + derive -------------------------------------------------
    clean = (
        valid.withColumn("unit_price", F.col("unit_price").cast(DecimalType(10, 2)))
        .withColumn("amount", (F.col("quantity") * F.col("unit_price")).cast(DecimalType(12, 2)))
        .withColumn("_processed_at", F.current_timestamp())
        .select(*SILVER_COLUMNS, "_processed_at")
    )
    n_clean = clean.count()
    n_rejected = rejected.count()
    metrics = {
        "in": n_in,
        "clean": n_clean,
        "rejected": n_rejected,
        "duplicates_removed": n_duplicates,
    }
    assert n_in == n_clean + n_rejected + n_duplicates, metrics
    return clean, rejected, metrics


def run(spark, bronze_path, silver_path, rejected_path):
    """Read Bronze, write Silver + quarantine, return the metrics dict."""
    bronze_df = spark.read.parquet(bronze_path)
    clean, rejected, metrics = clean_orders(bronze_df)
    clean.write.mode("overwrite").parquet(silver_path)
    rejected.write.mode("overwrite").parquet(rejected_path)
    return metrics


def main():
    ap = argparse.ArgumentParser(description="NEXUS Bronze -> Silver batch job")
    ap.add_argument("--csv", default=None,
                    help="raw orders CSV to land as Bronze first")
    ap.add_argument("--bronze", required=True, help="Bronze Parquet path")
    ap.add_argument("--silver", required=True, help="Silver Parquet path")
    ap.add_argument("--rejected", required=True, help="quarantine Parquet path")
    args = ap.parse_args()

    from src.processing.spark_session import get_spark

    spark = get_spark("nexus-bronze-to-silver")
    try:
        if args.csv:
            n = build_bronze(spark, args.csv, args.bronze)
            print(f"bronze: {n} rows landed from {args.csv}")
        metrics = run(spark, args.bronze, args.silver, args.rejected)
        print("silver metrics:", metrics)
        assert metrics["in"] == (
            metrics["clean"] + metrics["rejected"] + metrics["duplicates_removed"]
        )
        print("gate OK: in == clean + rejected + duplicates_removed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
