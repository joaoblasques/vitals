"""Phase 3 — PySpark-at-scale batch transform (the Databricks scale path).

The DuckDB silver step keeps the MVP clone-and-run; this is the same logic in PySpark, the way it
would run on Databricks against Delta at scale — including a Spark **window function** for a
per-patient rolling feature. Run: `python -m vitals.spark_silver` (needs pyspark).
"""
from __future__ import annotations

import os
from pathlib import Path

# Spark 4 supports Java 17/21 (not 24). Prefer a 17/21 JDK if present.
for _jh in ("/usr/local/opt/openjdk@17", "/usr/local/opt/openjdk@21"):
    if os.path.isdir(_jh):
        os.environ["JAVA_HOME"] = _jh
        break
os.environ.setdefault("JAVA_HOME", "/usr/local/opt/openjdk")

ROOT = Path(__file__).resolve().parents[2]
OBS = ROOT / "data" / "bronze" / "observations.ndjson"
OUT = ROOT / "data" / "spark" / "silver_observation"

MMOL_TO_MGDL = 18.0182


def run() -> dict:
    from pyspark.sql import SparkSession, Window
    from pyspark.sql import functions as F

    spark = (SparkSession.builder.master("local[2]").appName("vitals-spark-silver")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    raw = spark.read.json(str(OBS))  # schema inferred from the FHIR-shaped NDJSON

    flat = (raw
            .withColumn("patient_key", F.md5(F.regexp_replace("subject.reference", "Patient/", "")))
            .withColumn("obs_date", F.to_date("effectiveDateTime"))
            .withColumn("loinc", F.col("code.coding").getItem(0).getField("code"))
            # schema drift: value lives in valueQuantity.value OR a flat value column
            .withColumn("raw_value", F.coalesce(F.col("valueQuantity.value"), F.col("value")))
            .withColumn("raw_unit", F.coalesce(F.col("valueQuantity.unit"), F.col("unit")))
            .filter(F.col("raw_value").isNotNull()))

    # standardize glucose mmol/L -> mg/dL
    std = flat.withColumn(
        "value_std",
        F.when((F.col("loinc") == "2339-0") & (F.lower("raw_unit") == "mmol/l"),
               F.round(F.col("raw_value") * MMOL_TO_MGDL, 1)).otherwise(F.col("raw_value")),
    ).withColumn("metric", F.expr(
        "CASE loinc WHEN '2339-0' THEN 'glucose' WHEN '8867-4' THEN 'heart_rate' "
        "WHEN '38208-5' THEN 'pain' WHEN 'adherence-pct' THEN 'adherence' ELSE 'other' END"))

    # Spark WINDOW FUNCTION: 7-observation rolling mean of pain per patient (at-scale feature).
    w = Window.partitionBy("patient_key").orderBy("obs_date").rowsBetween(-6, 0)
    feat = (std.filter(F.col("metric") == "pain")
            .withColumn("pain_rolling7", F.round(F.avg("value_std").over(w), 2))
            .select("patient_key", "obs_date", "value_std", "pain_rolling7"))

    feat.write.mode("overwrite").parquet(str(OUT))
    n = feat.count()
    sample = [r.asDict() for r in feat.orderBy("patient_key", "obs_date").limit(4).collect()]
    spark.stop()
    result = {"rows": n, "sink": "data/spark/silver_observation (parquet)",
              "window_feature": "pain_rolling7 (7-obs rolling mean per patient)", "sample": sample}
    print("spark silver complete:", result)
    return result


if __name__ == "__main__":
    run()
