"""Spark batch Bronze->Silver (Phase 3 scaffold, runnable in Spark local mode)."""
def run(spark, bronze_path, silver_path):
    from pyspark.sql import functions as F
    df=spark.read.parquet(bronze_path)
    clean=(df.dropDuplicates(["order_id"]).withColumn("unit_price",F.col("unit_price").cast("decimal(10,2)")).filter(F.col("quantity")>0))
    clean.write.mode("overwrite").parquet(silver_path)
    return clean.count()
