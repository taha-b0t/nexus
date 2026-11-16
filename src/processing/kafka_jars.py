"""Local Kafka connector jars for Structured Streaming.

Spark's `spark.jars.packages` (Ivy) cannot reach Maven Central from this
VM: the JVM ignores the HTTP proxy env vars, so Ivy's direct HTTPS hits
the sandbox gateway and dies with "Unsupported or unrecognized SSL
message". curl/urllib (proxy-aware) work fine, so we download the jars
once into ~/.cache/nexus-jars and hand them to Spark as local files via
`spark.jars`. On a machine where Ivy works, the streaming job falls back
to `spark.jars.packages` when the download fails.
"""
import os
import urllib.request
from pathlib import Path

MAVEN_BASE = "https://repo1.maven.org/maven2"
CACHE_DIR = Path(os.environ.get("NEXUS_JARS_DIR", Path.home() / ".cache" / "nexus-jars"))

JARS = [
    "org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar",
    "org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar",
    "org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar",
    "org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar",
    "org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar",
]


def ensure_kafka_jars(cache_dir=CACHE_DIR):
    """Download any missing connector jars; return the list of local paths.

    Raises RuntimeError if a jar cannot be fetched (caller falls back to
    spark.jars.packages or skips the Kafka-source check).
    """
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for rel in JARS:
        dest = cache_dir / Path(rel).name
        if not dest.exists() or dest.stat().st_size == 0:
            url = f"{MAVEN_BASE}/{rel}"
            try:
                urllib.request.urlretrieve(url, dest)
            except Exception as exc:  # noqa: BLE001 - reported to caller
                if dest.exists():
                    dest.unlink(missing_ok=True)
                raise RuntimeError(f"could not download {url}: {exc}") from exc
        paths.append(str(dest))
    return paths
