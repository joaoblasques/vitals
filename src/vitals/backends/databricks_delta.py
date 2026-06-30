"""Databricks backend — land bronze as Delta tables in Unity Catalog.

Bronze-first slice of the Delta-on-UC writer (see
docs/superpowers/specs/2026-06-26-delta-on-uc-writer-design.md). Uploads the raw NDJSON into the
`vitals_bronze.raw.landing` UC volume, then writes one Delta table per source into
`vitals_bronze.raw.*` — raw and as-is, schema-inferred (FHIR nesting preserved), no de-id yet
(PHI boundary is still at silver).

Execution: databricks-connect against Free Edition serverless (ADR 0005). Auth comes from the
DATABRICKS_HOST / DATABRICKS_TOKEN env vars (source infra/terraform/.env first).

I/O (upload + Spark writes) is kept separate from the pure parity check (`parity_report`) so the
logic is unit-testable without a workspace.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from vitals import env
from vitals.vocab import ICD_DISPLAY, MMOL_TO_MGDL, TEXT_TO_ICD

ROOT = Path(__file__).resolve().parents[3]
SILVER_BASELINE = ROOT / "data" / "silver_baseline.json"  # written by vitals.lakehouse

CATALOG = "vitals_bronze"
SCHEMA = "raw"
VOLUME = "landing"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# The eight raw sources landed in bronze (mirrors vitals.lakehouse).
SOURCES = [
    "patients", "encounters", "conditions", "observations",
    "notes", "claims", "pro_surveys", "wearables",
]

# Silver: de-identified, conformed clinical entities (PHI boundary is crossed here).
SILVER_CATALOG = "vitals_silver"
SILVER_SCHEMA = "clinical"
SILVER_TABLES = [
    "patient", "target", "observation", "condition", "note", "claim", "pro", "wearable_daily",
]
# Columns that must NEVER appear in silver.patient (HIPAA Safe Harbor identifiers).
PHI_COLUMNS = {"name", "identifier", "address", "birthDate", "ssn"}

# Gold: built by dbt (`dbt build --target databricks`) into vitals_gold.marts. Parity is checked
# against a baseline of the local DuckDB gold (written by `gold-baseline`).
GOLD_CATALOG = "vitals_gold"
GOLD_SCHEMA = "marts"
GOLD_BASELINE = ROOT / "data" / "gold_baseline.json"
LOCAL_DB = ROOT / "data" / "vitals.duckdb"

# Monitoring: PSI drift on the gold marts, landed in vitals_gold.monitoring.drift_report.
MONITORING_SCHEMA = "monitoring"
DRIFT_TABLE = "drift_report"
DRIFT_BASELINE = ROOT / "data" / "drift_report.json"  # local monitor output (parity baseline)


# ---- pure logic (unit-testable, no I/O) -------------------------------------------------------

def parity_report(local: dict[str, int], remote: dict[str, int]) -> dict[str, dict]:
    """Compare local (DuckDB/NDJSON) vs remote (Delta) row counts per source.

    Returns {source: {local, remote, match}}. The acceptance gate for the bronze slice is that
    every source matches (project principle: verify every step against row counts).
    """
    report = {}
    for name in sorted(set(local) | set(remote)):
        lc, rc = local.get(name), remote.get(name)
        report[name] = {"local": lc, "remote": rc, "match": lc == rc and lc is not None}
    return report


def all_match(report: dict[str, dict]) -> bool:
    return bool(report) and all(r["match"] for r in report.values())


def local_counts() -> dict[str, int]:
    """Row count per source from the local NDJSON (the bronze parity baseline)."""
    counts = {}
    for name in SOURCES:
        path = env.bronze_dir() / f"{name}.ndjson"
        with path.open("rb") as fh:
            counts[name] = sum(1 for _ in fh)
    return counts


# ---- I/O (requires a live workspace) ----------------------------------------------------------

def _spark():
    if env.spark_mode() == "ambient":
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()        # ON Databricks: ambient serverless session
    from databricks.connect import DatabricksSession
    return DatabricksSession.builder.serverless().getOrCreate()  # connect from laptop (unchanged)


def _upload_landing() -> None:
    """Upload each raw NDJSON into the bronze landing volume (overwrite = idempotent)."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    for name in SOURCES:
        local = env.bronze_dir() / f"{name}.ndjson"
        with local.open("rb") as fh:
            w.files.upload(f"{VOLUME_PATH}/{name}.ndjson", fh, overwrite=True)


def land_bronze() -> dict[str, int]:
    """Upload NDJSON to the volume, write one Delta table per source, return remote row counts.

    Idempotent: files overwrite, tables are CREATE OR REPLACE via mode('overwrite').
    """
    _upload_landing()
    spark = _spark()
    spark.sql(f"USE CATALOG {CATALOG}")
    spark.sql(f"USE SCHEMA {SCHEMA}")

    counts = {}
    for name in SOURCES:
        df = spark.read.json(f"{VOLUME_PATH}/{name}.ndjson")
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            f"{CATALOG}.{SCHEMA}.{name}"
        )
        counts[name] = spark.table(f"{CATALOG}.{SCHEMA}.{name}").count()
    return counts


# ---- silver: bronze Delta -> de-identified, conformed Delta -------------------------------------

def _silver_statements() -> list[tuple[str, str]]:
    """(table, Spark SQL) for each silver table — the SAME conform/de-id logic as the DuckDB silver
    (vitals.lakehouse), translated to Spark dialect (0-indexed arrays, date_add, backticked
    reserved words). The PHI boundary is enforced in `patient`: identifiers are dropped, only a
    hashed surrogate key + coarse age + a per-patient date shift survive."""
    b = f"{CATALOG}.{SCHEMA}"                      # vitals_bronze.raw
    s = f"{SILVER_CATALOG}.{SILVER_SCHEMA}"        # vitals_silver.clinical
    cases = " ".join(f"WHEN lower(code.text)='{t}' THEN '{c}'" for t, c in TEXT_TO_ICD.items())
    disp = " ".join(f"WHEN '{c}' THEN '{d}'" for c, d in ICD_DISPLAY.items())
    return [
        # PHI boundary: drop name/identifier/address/birthDate; keep hashed key, gender, capped age,
        # and a deterministic per-patient date shift (preserves intervals, de-identifies dates).
        ("patient", f"""CREATE OR REPLACE TABLE {s}.patient AS
            WITH dedup AS (
                SELECT *, row_number() OVER (PARTITION BY id ORDER BY id) AS rn FROM {b}.patients
            )
            SELECT md5(id) AS patient_key,
                   gender,
                   CASE WHEN birthDate IS NULL THEN NULL
                        ELSE least(90, 2026 - year(to_date(birthDate))) END AS age,
                   CAST(conv(substr(md5(id),1,8),16,10) % 21 AS INT) - 10 AS _date_shift_days
            FROM dedup WHERE rn = 1"""),
        ("target", f"""CREATE OR REPLACE TABLE {s}.target AS
            SELECT DISTINCT md5(id) AS patient_key, CAST(`_label_surgery_90d` AS INT) AS surgery_90d
            FROM {b}.patients WHERE `_label_surgery_90d` IS NOT NULL"""),
        ("observation", f"""CREATE OR REPLACE TABLE {s}.observation AS
            WITH flat AS (
                SELECT md5(replace(subject.reference,'Patient/','')) AS patient_key,
                       to_date(effectiveDateTime) AS obs_date_raw,
                       code.coding[0].code AS loinc_code,
                       code.coding[0].display AS display,
                       coalesce(valueQuantity.value, value) AS raw_value,
                       coalesce(valueQuantity.unit, unit) AS raw_unit
                FROM {b}.observations
            )
            SELECT f.patient_key,
                   date_add(f.obs_date_raw, p._date_shift_days) AS obs_date,
                   f.loinc_code, f.display,
                   CASE WHEN f.loinc_code='2339-0' AND lower(f.raw_unit)='mmol/l'
                        THEN round(f.raw_value * {MMOL_TO_MGDL}, 1) ELSE f.raw_value END AS value_std,
                   CASE WHEN f.loinc_code='2339-0' THEN 'mg/dL' ELSE f.raw_unit END AS unit_std,
                   CASE f.loinc_code WHEN '2339-0' THEN 'glucose' WHEN '8867-4' THEN 'heart_rate'
                        WHEN '38208-5' THEN 'pain' WHEN 'adherence-pct' THEN 'adherence'
                        ELSE 'other' END AS metric
            FROM flat f JOIN {s}.patient p USING (patient_key)
            WHERE f.raw_value IS NOT NULL"""),
        ("condition", f"""CREATE OR REPLACE TABLE {s}.condition AS
            WITH base AS (
                SELECT md5(replace(subject.reference,'Patient/','')) AS patient_key,
                       coalesce(code.coding[0].code, CASE {cases} ELSE NULL END) AS icd10_code,
                       (code.coding IS NULL AND code.text IS NOT NULL) AS recovered_from_text
                FROM {b}.conditions
            )
            SELECT patient_key, icd10_code,
                   CASE icd10_code {disp} ELSE icd10_code END AS display,
                   recovered_from_text
            FROM base"""),
        ("note", f"""CREATE OR REPLACE TABLE {s}.note AS
            SELECT md5(replace(subject.reference,'Patient/','')) AS patient_key,
                   to_date(`date`) AS note_date, text
            FROM {b}.notes WHERE text IS NOT NULL"""),
        ("claim", f"""CREATE OR REPLACE TABLE {s}.claim AS
            SELECT md5(replace(c.patient.reference,'Patient/','')) AS patient_key,
                   date_add(to_date(c.billablePeriod.start), p._date_shift_days) AS claim_date,
                   c.`procedure`[0].code AS procedure_code,
                   c.`procedure`[0].display AS procedure_display,
                   c.diagnosis[0].code AS dx_code,
                   try_cast(cast(c.total.value AS string) AS double) AS billed,
                   c.paid AS paid, c.status AS status, (c.status = 'denied') AS denied
            FROM {b}.claims c
            JOIN {s}.patient p ON p.patient_key = md5(replace(c.patient.reference,'Patient/',''))"""),
        ("pro", f"""CREATE OR REPLACE TABLE {s}.pro AS
            SELECT md5(replace(j.subject.reference,'Patient/','')) AS patient_key,
                   date_add(to_date(j.authored), p._date_shift_days) AS survey_date,
                   j.instrument,
                   CASE WHEN j.score BETWEEN 0 AND 100 THEN j.score ELSE NULL END AS score
            FROM {b}.pro_surveys j
            JOIN {s}.patient p ON p.patient_key = md5(replace(j.subject.reference,'Patient/',''))"""),
        ("wearable_daily", f"""CREATE OR REPLACE TABLE {s}.wearable_daily AS
            SELECT md5(replace(w.patient.reference,'Patient/','')) AS patient_key,
                   date_add(to_date(w.`date`), p._date_shift_days) AS day,
                   CASE WHEN w.steps BETWEEN 0 AND 50000 THEN w.steps ELSE NULL END AS steps,
                   w.active_minutes, w.resting_hr, w.sleep_hours
            FROM {b}.wearables w
            JOIN {s}.patient p ON p.patient_key = md5(replace(w.patient.reference,'Patient/',''))"""),
    ]


def build_silver() -> dict[str, int]:
    """Build all silver Delta tables in vitals_silver.clinical; return per-table row counts.
    Idempotent (CREATE OR REPLACE). `patient` must build first — the others join to it."""
    spark = _spark()
    counts = {}
    for name, sql in _silver_statements():
        spark.sql(sql)
        counts[name] = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.{name}").count()
    return counts


def silver_patient_columns() -> list[str]:
    return _spark().table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.patient").columns


def assert_no_phi(columns: list[str]) -> None:
    """The project's signature check: no HIPAA identifiers survive into silver.patient."""
    leaked = set(columns) & PHI_COLUMNS
    if leaked:
        raise AssertionError(f"PHI leaked into silver.patient: {sorted(leaked)}")


def silver_baseline() -> dict[str, int]:
    """Local DuckDB silver per-table counts (written by vitals.lakehouse). Parity baseline."""
    return json.loads(SILVER_BASELINE.read_text())


def _print_parity(title: str, report: dict[str, dict]) -> bool:
    print(f"\n  {title:<28} {'local':>8} {'remote':>8}  match")
    for name, r in report.items():
        print(f"  {name:<28} {str(r['local']):>8} {str(r['remote']):>8}  "
              f"{'OK' if r['match'] else 'MISMATCH'}")
    return all_match(report)


# ---- gold: parity of the dbt-built gold (vitals_gold.marts) vs local DuckDB gold ---------------

def write_local_gold_baseline() -> dict[str, int]:
    """Dump local DuckDB `gold.*` row counts to GOLD_BASELINE (run in the main venv, has duckdb).
    Introspects the schema so new models are picked up automatically."""
    import duckdb  # lazy: only the local venv has duckdb (the connect venv does not)

    con = duckdb.connect(str(LOCAL_DB))
    tbls = [r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='gold' ORDER BY 1"
    ).fetchall()]
    counts = {t: con.execute(f"SELECT count(*) FROM gold.{t}").fetchone()[0] for t in tbls}
    con.close()
    GOLD_BASELINE.write_text(json.dumps(counts, indent=2))
    print(f"wrote {GOLD_BASELINE.name}: {len(counts)} gold tables")
    return counts


def gold_baseline() -> dict[str, int]:
    return json.loads(GOLD_BASELINE.read_text())


def verify_gold() -> dict[str, int]:
    """Remote row counts for the dbt-built gold tables in vitals_gold.marts."""
    spark = _spark()
    return {t: spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.{t}").count() for t in gold_baseline()}


def main_gold() -> None:
    print(f"[gold parity] {GOLD_CATALOG}.{GOLD_SCHEMA} vs local DuckDB gold "
          "(build first: dbt build --target databricks) ...")
    ok = _print_parity("table", parity_report(gold_baseline(), verify_gold()))
    print(f"\n{'✅ gold parity: all tables match local DuckDB' if ok else '❌ gold parity FAILED'}")
    if not ok:
        raise SystemExit(1)


def main_bronze() -> None:
    print(f"[bronze->delta] landing {len(SOURCES)} sources into {CATALOG}.{SCHEMA} ...")
    ok = _print_parity("source", parity_report(local_counts(), land_bronze()))
    print(f"\n{'✅ bronze parity: all sources match' if ok else '❌ bronze parity FAILED'}")
    if not ok:
        raise SystemExit(1)


def main_silver() -> None:
    print(f"[silver->delta] building {len(SILVER_TABLES)} tables in {SILVER_CATALOG}.{SILVER_SCHEMA} ...")
    remote = build_silver()
    cols = silver_patient_columns()
    assert_no_phi(cols)  # PHI boundary — fail hard before reporting parity
    print(f"  PHI boundary OK — silver.patient columns: {cols}")
    ok = _print_parity("table", parity_report(silver_baseline(), remote))
    print(f"\n{'✅ silver parity: all tables match local DuckDB' if ok else '❌ silver parity FAILED'}")
    if not ok:
        raise SystemExit(1)


# ---- monitoring: PSI drift on the gold marts -> vitals_gold.monitoring.drift_report -----------

# The 8 MONITORED features per patient, computed from the gold marts with the SAME semantics as the
# local FEATURE_SQL (vitals.serve): inner-join on observations (a patient must have measurements),
# left-join the rest, claims coalesced to 0. Spark dialect — FILTER (WHERE ...) is supported on UC.
DRIFT_FEATURE_SQL = f"""
WITH obs AS (
    SELECT patient_key,
        avg(value_std) FILTER (WHERE metric = 'pain')      AS mean_pain,
        avg(value_std) FILTER (WHERE metric = 'adherence') AS mean_adherence,
        avg(value_std) FILTER (WHERE metric = 'glucose')   AS mean_glucose_mgdl
    FROM {GOLD_CATALOG}.{GOLD_SCHEMA}.fct_observation GROUP BY 1
),
clm AS (
    SELECT patient_key, sum(coalesce(paid, 0)) AS total_paid
    FROM {GOLD_CATALOG}.{GOLD_SCHEMA}.fct_claim GROUP BY 1
),
pro AS (
    SELECT patient_key, avg(score) AS mean_odi
    FROM {GOLD_CATALOG}.{GOLD_SCHEMA}.fct_pro GROUP BY 1
),
wbl AS (
    SELECT patient_key, avg(steps) AS mean_steps, avg(active_minutes) AS mean_active_min
    FROM {GOLD_CATALOG}.{GOLD_SCHEMA}.fct_wearable_daily GROUP BY 1
)
SELECT d.patient_key, d.age,
       o.mean_pain, o.mean_adherence, o.mean_glucose_mgdl,
       coalesce(clm.total_paid, 0) AS total_paid,
       pro.mean_odi, wbl.mean_steps, wbl.mean_active_min
FROM {GOLD_CATALOG}.{GOLD_SCHEMA}.dim_patient d
JOIN obs o USING (patient_key)
LEFT JOIN clm USING (patient_key)
LEFT JOIN pro USING (patient_key)
LEFT JOIN wbl USING (patient_key)
"""


def drift_rows(report: dict) -> list[tuple]:
    """Flatten the nested drift report into tidy (split, feature, psi, band, is_alert) rows —
    the shape a monitoring dashboard actually queries. Pure, so it's unit-testable."""
    rows = []
    for split in ("stable_split", "shifted_population"):
        for feat, v in report[split].items():
            rows.append((split, feat, float(v["psi"]), v["band"], v["band"] != "stable"))
    return rows


def write_drift(spark, report: dict) -> int:
    """Append the drift report as tidy rows to vitals_gold.monitoring.drift_report; return row count.

    Append-only history: each run is a point-in-time snapshot stamped run_ts server-side, so the
    table is a drift time series you can trend — not a single overwritten row."""
    from pyspark.sql.functions import current_timestamp
    from pyspark.sql.types import (BooleanType, DoubleType, StringType, StructField, StructType)

    schema = StructType([
        StructField("split", StringType(), False),
        StructField("feature", StringType(), False),
        StructField("psi", DoubleType(), False),
        StructField("band", StringType(), False),
        StructField("is_alert", BooleanType(), False),
    ])
    df = spark.createDataFrame(drift_rows(report), schema).withColumn("run_ts", current_timestamp())
    df.write.mode("append").saveAsTable(f"{GOLD_CATALOG}.{MONITORING_SCHEMA}.{DRIFT_TABLE}")
    return df.count()


def build_drift(spark) -> dict:
    """Compute PSI drift from the gold marts and append it to the monitoring table.

    Engine-agnostic: the caller passes the SparkSession (databricks-connect for the dev/parity path,
    the ambient serverless session on the scheduled job). The PSI math is `vitals.drift.build_report`
    — the exact same code the local monitor runs, so the two can't diverge."""
    from vitals.drift import build_report

    feats = spark.sql(DRIFT_FEATURE_SQL).toPandas()
    report = build_report(feats)
    report["_rows_written"] = write_drift(spark, report)
    return report


def drift_baseline() -> dict:
    """The local monitor's drift_report.json — the parity baseline (run `make monitor` to refresh)."""
    return json.loads(DRIFT_BASELINE.read_text())


def drift_parity(local: dict, remote: dict, tol: float = 1e-3) -> dict[str, dict]:
    """Compare PSI per (split.feature) between the local monitor and the Databricks run.

    Same underlying data (gold parity already verified), same PSI math — so values should match
    within floating-point tolerance across the DuckDB vs Spark aggregation of the features."""
    report = {}
    for split in ("stable_split", "shifted_population"):
        keys = set(local.get(split, {})) | set(remote.get(split, {}))
        for feat in sorted(keys):
            lp = local.get(split, {}).get(feat, {}).get("psi")
            rp = remote.get(split, {}).get(feat, {}).get("psi")
            match = lp is not None and rp is not None and abs(lp - rp) <= tol
            report[f"{split}.{feat}"] = {"local": lp, "remote": rp, "match": match}
    return report


def main_drift() -> None:
    print(f"[drift] PSI on {GOLD_CATALOG}.{GOLD_SCHEMA} -> "
          f"{GOLD_CATALOG}.{MONITORING_SCHEMA}.{DRIFT_TABLE} ...")
    remote = build_drift(_spark())
    print(f"  wrote {remote['_rows_written']} rows; alerts: {remote['alerts']}")
    ok = _print_parity("psi (split.feature)", drift_parity(drift_baseline(), remote))
    print(f"\n{'✅ drift parity: PSI matches the local monitor' if ok else '❌ drift parity FAILED'}")
    if not ok:
        raise SystemExit(1)


def main() -> None:
    stage = sys.argv[1] if len(sys.argv) > 1 else "bronze"
    if stage == "bronze":
        main_bronze()
    elif stage == "silver":
        main_silver()
    elif stage == "gold":
        main_gold()
    elif stage == "gold-baseline":
        write_local_gold_baseline()
    elif stage == "drift":
        main_drift()
    elif stage == "all":
        main_bronze()
        main_silver()
    else:
        raise SystemExit(
            f"unknown stage {stage!r} (use: bronze | silver | gold | gold-baseline | drift | all)")


if __name__ == "__main__":
    main()
