"""Shared SparkSession factory for NEXUS Phase 3+ jobs.

Sandbox notes (verified 2026-11-16 on this VM):
- PySpark runs in local mode with the JDK at ~/jdk-11.0.32.1+1 and the Spark
  3.5.3 distribution at ~/spark-3.5.3-bin-hadoop3. JAVA_HOME / SPARK_HOME env
  vars win when set; the local paths below are only fallbacks when the env
  vars are unset AND the directories exist, so a laptop with a normal Java
  install is unaffected.
- Python-worker paths are BROKEN in this sandbox (the forked Python worker's
  socket dies with "Broken pipe"): createDataFrame() from Python objects,
  Python UDFs, RDD map with Python functions and toPandas() all fail. Pure
  JVM-side DataFrame ops (spark.range, spark.read.*, built-in functions,
  collect() back to the driver) work fine. Every job and test in this repo
  must use JVM-side ops only; never a Python UDF.
- Spark binds its driver to the machine hostname by default, which resolves
  to the sandbox gateway IP and breaks task sockets. Pin the driver to
  127.0.0.1 (SPARK_LOCAL_IP env var also works).
"""
import os
from pathlib import Path

DEFAULT_DRIVER_MEMORY = "1g"
DEFAULT_SHUFFLE_PARTITIONS = "2"


def _first_existing(*candidates):
    for c in candidates:
        p = Path(c).expanduser()
        if p.is_dir():
            return str(p)
    return None


def resolve_java_home():
    """Return a usable JAVA_HOME: env var first, else known local JDKs."""
    if os.environ.get("JAVA_HOME"):
        return os.environ["JAVA_HOME"]
    found = _first_existing(
        Path.home() / "jdk-11.0.32.1+1",
        "/usr/lib/jvm/java-17-openjdk-amd64",
        "/usr/lib/jvm/java-11-openjdk-amd64",
    )
    if found:
        os.environ["JAVA_HOME"] = found
    return os.environ.get("JAVA_HOME")


def resolve_spark_home():
    """Return a usable SPARK_HOME: env var first, else known local distros."""
    if os.environ.get("SPARK_HOME"):
        return os.environ["SPARK_HOME"]
    found = _first_existing(
        Path.home() / "spark-3.5.3-bin-hadoop3",
        "/opt/spark",
    )
    if found:
        os.environ["SPARK_HOME"] = found
    return os.environ.get("SPARK_HOME")


def get_spark(app_name, extra_packages=None, extra_jars=None):
    """Build a local-mode SparkSession with NEXUS defaults.

    extra_packages: Maven coordinates, e.g.
        ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"]
    extra_jars: local jar paths (preferred on this VM: Ivy cannot use the
        HTTP proxy, see src/processing/kafka_jars.py).
    """
    resolve_java_home()
    resolve_spark_home()
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", DEFAULT_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", DEFAULT_SHUFFLE_PARTITIONS)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.enabled", "false")
    )
    if extra_packages:
        builder = builder.config(
            "spark.jars.packages", ",".join(extra_packages))
    if extra_jars:
        # Classpath, NOT spark.jars: spark.jars distributes jars over the
        # driver's Netty RPC, and this sandbox's gateway corrupts that
        # stream (verified 2026-11-16: "Too large frame" whose length
        # decodes to ASCII proxy text). In local mode driver == executor,
        # so extraClassPath on both is enough and no distribution happens.
        cp = ":".join(extra_jars)
        builder = builder.config("spark.driver.extraClassPath", cp)
        builder = builder.config("spark.executor.extraClassPath", cp)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
