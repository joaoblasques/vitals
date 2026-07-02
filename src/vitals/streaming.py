"""Phase 3 — Spark Structured Streaming for the wearable feed.

Wearables arrive continuously in production; batch isn't enough. This job consumes them as a
stream, cleans outliers on the fly, and writes a cleaned Parquet stream with checkpointing.

The source here is a *file* stream (a landing directory of micro-batch JSON files) so the demo
runs with no broker. **In production the only change is the source**:
    .readStream.format("kafka").option("subscribe", "wearables") ...
instead of `.readStream.schema(...).json(landing)`. Everything downstream is identical.

Run: `python -m vitals.streaming`  (needs the `[databricks]` extra: pyspark).
"""
from __future__ import annotations

import os
import shutil
from pathlib import Path

# Spark 4 supports Java 17/21 (not 24). Prefer a 17/21 JDK if present.
for _jh in ("/usr/local/opt/openjdk@17", "/usr/local/opt/openjdk@21"):
    if os.path.isdir(_jh):
        os.environ["JAVA_HOME"] = _jh
        break
os.environ.setdefault("JAVA_HOME", "/usr/local/opt/openjdk")

ROOT = Path(__file__).resolve().parents[2]
BRONZE_WEARABLES = ROOT / "data" / "bronze" / "wearables.ndjson"
LANDING = ROOT / "data" / "stream" / "landing"
OUT = ROOT / "data" / "stream" / "cleaned"
CHECKPOINT = ROOT / "data" / "stream" / "checkpoint"


def kafka_connector_package(version: str | None = None) -> str:
    """Maven coordinate for the spark-sql-kafka connector, matching the installed pyspark: Scala 2.13
    for Spark 4+, 2.12 for Spark 3.x; connector version == pyspark version (they must line up)."""
    if version is None:
        import pyspark
        version = pyspark.__version__
    scala = "2.13" if int(version.split(".")[0]) >= 4 else "2.12"
    return f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{version}"


def _schema():
    from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
    return StructType([
        StructField("type", StringType()),
        StructField("id", StringType()),
        StructField("patient", StructType([StructField("reference", StringType())])),
        StructField("date", StringType()),
        StructField("steps", LongType()),
        StructField("active_minutes", LongType()),
        StructField("resting_hr", LongType()),
        StructField("sleep_hours", DoubleType()),
    ])


# SCHEMA is the builder function; call SCHEMA() to obtain the StructType (keeps module hermetic).
SCHEMA = _schema


def clean_wearables(stream):
    """The shared cleaning transform (identical for the file + Kafka sources): derive patient_key +
    event_date, null out impossible step counts, select the canonical columns."""
    from pyspark.sql import functions as F
    return (stream
            .withColumn("patient_key", F.md5(F.regexp_replace("patient.reference", "Patient/", "")))
            .withColumn("event_date", F.to_date("date"))
            .withColumn("steps", F.when((F.col("steps") >= 0) & (F.col("steps") <= 50000), F.col("steps")))
            .select("patient_key", "event_date", "steps", "active_minutes", "resting_hr", "sleep_hours"))


def produce_stream(n_batches: int = 6) -> int:
    """Split the wearable bronze file into N micro-batch JSON files (simulated arrival)."""
    LANDING.mkdir(parents=True, exist_ok=True)
    for p in LANDING.glob("*.json"):
        p.unlink()
    lines = BRONZE_WEARABLES.read_text().splitlines()
    size = max(1, len(lines) // n_batches)
    for i in range(n_batches):
        chunk = lines[i * size:(i + 1) * size] if i < n_batches - 1 else lines[i * size:]
        (LANDING / f"batch_{i:02d}.json").write_text("\n".join(chunk) + "\n")
    print(f"produced {n_batches} micro-batches ({len(lines)} events) -> {LANDING}")
    return len(lines)


def run_stream() -> dict:
    from pyspark.sql import SparkSession

    shutil.rmtree(OUT, ignore_errors=True)
    shutil.rmtree(CHECKPOINT, ignore_errors=True)

    spark = (SparkSession.builder.master("local[2]").appName("vitals-wearable-stream")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    # File-source streaming needs an explicit schema (same schema a Kafka value would deserialize to).
    schema = _schema()

    stream = (spark.readStream.schema(schema).option("maxFilesPerTrigger", 1)
              .json(str(LANDING)))

    cleaned = clean_wearables(stream)

    query = (cleaned.writeStream.format("parquet")
             .option("path", str(OUT))
             .option("checkpointLocation", str(CHECKPOINT))
             .outputMode("append")
             .trigger(availableNow=True)   # process all available micro-batches, then stop
             .start())
    query.awaitTermination()

    out = spark.read.parquet(str(OUT))
    n = out.count()
    outliers = out.filter("steps is null").count()
    sample = [r.asDict() for r in out.limit(3).collect()]
    spark.stop()
    result = {"events_streamed": n, "outliers_nulled": outliers, "sink": "data/stream/cleaned (parquet)",
              "sample": sample}
    print("streaming complete:", result)
    return result


def main() -> dict:
    produce_stream()
    return run_stream()


if __name__ == "__main__":
    main()
