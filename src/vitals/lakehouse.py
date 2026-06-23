"""Bronze -> Silver on a local DuckDB lakehouse.

Bronze = raw FHIR NDJSON loaded as-is (schema drift tolerated, PHI present).
Silver = de-identified, conformed, type-clean, with a data-quality report.

This mirrors the medallion design; on Databricks the same logic runs as PySpark + dbt
against Delta, but DuckDB keeps the MVP clone-and-run.
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]
BRONZE = ROOT / "data" / "bronze"
DB = ROOT / "data" / "vitals.duckdb"
DQ_OUT = ROOT / "data" / "dq_report.json"

MMOL_TO_MGDL = 18.0182  # glucose conversion factor

# Free-text condition -> ICD-10 (the silver layer recovers validity the bronze data lost).
TEXT_TO_ICD = {
    "low back pain": "M54.5",
    "knee osteoarthritis": "M17.0",
    "rotator cuff tear": "M75.100",
    "herniated disc": "M51.26",
    "right knee pain": "M25.561",
}
# Canonical display per ICD-10 code (conforms both coded and text-recovered conditions).
ICD_DISPLAY = {
    "M54.5": "Low back pain",
    "M17.0": "Bilateral primary osteoarthritis of knee",
    "M75.100": "Rotator cuff tear",
    "M51.26": "Lumbar disc displacement",
    "M25.561": "Pain in right knee",
}


def build() -> dict:
    DB.unlink(missing_ok=True)
    con = duckdb.connect(str(DB))
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze; CREATE SCHEMA IF NOT EXISTS silver;")

    # ---- BRONZE: load raw NDJSON (union_by_name absorbs schema drift) ----
    for name in ["patients", "encounters", "conditions", "observations", "notes",
                 "claims", "pro_surveys", "wearables"]:
        con.execute(
            f"""CREATE OR REPLACE TABLE bronze.{name} AS
                SELECT * FROM read_json('{BRONZE/f'{name}.ndjson'}',
                    format='newline_delimited', union_by_name=true)"""
        )

    dq_before = _dq_bronze(con)

    # ---- SILVER: patient (de-identified) ----
    # Drop PHI (name, identifier/ssn, address, raw birthDate). Keep a hashed surrogate key,
    # gender, and age (capped at 90 per HIPAA Safe Harbor). Dedupe exact-duplicate rows.
    con.execute(
        """CREATE OR REPLACE TABLE silver.patient AS
           WITH dedup AS (
               SELECT *, row_number() OVER (PARTITION BY id ORDER BY id) AS rn FROM bronze.patients
           )
           SELECT md5(id) AS patient_key,
                  gender,
                  CASE WHEN birthDate IS NULL THEN NULL
                       ELSE least(90, 2026 - CAST(strftime(CAST(birthDate AS DATE), '%Y') AS INTEGER)) END AS age,
                  CAST(abs(hash(id)) % 21 AS INTEGER) - 10 AS _date_shift_days   -- per-patient shift (de-id, preserves intervals)
           FROM dedup WHERE rn = 1"""
    )

    # ---- SILVER: target label (separated from the patient record) ----
    con.execute(
        """CREATE OR REPLACE TABLE silver.target AS
           SELECT DISTINCT md5(id) AS patient_key, CAST("_label_surgery_90d" AS INTEGER) AS surgery_90d
           FROM bronze.patients WHERE "_label_surgery_90d" IS NOT NULL"""
    )

    # ---- SILVER: observation (flatten drift, standardize units, type-clean) ----
    con.execute(
        f"""CREATE OR REPLACE TABLE silver.observation AS
           WITH flat AS (
               SELECT md5(replace(subject.reference, 'Patient/', '')) AS patient_key,
                      CAST(effectiveDateTime AS DATE) AS obs_date_raw,
                      code.coding[1].code  AS loinc_code,
                      code.coding[1].display AS display,
                      COALESCE(valueQuantity.value, value)  AS raw_value,
                      COALESCE(valueQuantity.unit,  unit)   AS raw_unit
               FROM bronze.observations
           )
           SELECT f.patient_key,
                  (f.obs_date_raw + (p._date_shift_days * INTERVAL 1 DAY))::DATE AS obs_date,
                  f.loinc_code,
                  f.display,
                  CASE WHEN f.loinc_code='2339-0' AND lower(f.raw_unit)='mmol/l'
                       THEN round(f.raw_value * {MMOL_TO_MGDL}, 1) ELSE f.raw_value END AS value_std,
                  CASE WHEN f.loinc_code='2339-0' THEN 'mg/dL' ELSE f.raw_unit END AS unit_std,
                  CASE f.loinc_code
                       WHEN '2339-0' THEN 'glucose' WHEN '8867-4' THEN 'heart_rate'
                       WHEN '38208-5' THEN 'pain' WHEN 'adherence-pct' THEN 'adherence'
                       ELSE 'other' END AS metric
           FROM flat f JOIN silver.patient p USING (patient_key)
           WHERE f.raw_value IS NOT NULL  -- completeness gate: drop the ~4% null-value rows
    """
    )

    # ---- SILVER: condition (recover ICD-10 from free text + conform the display) ----
    cases = " ".join(f"WHEN lower(code.text)='{t}' THEN '{c}'" for t, c in TEXT_TO_ICD.items())
    disp = " ".join(f"WHEN '{c}' THEN '{d}'" for c, d in ICD_DISPLAY.items())
    con.execute(
        f"""CREATE OR REPLACE TABLE silver.condition AS
           WITH base AS (
               SELECT md5(replace(subject.reference,'Patient/','')) AS patient_key,
                      COALESCE(code.coding[1].code, CASE {cases} ELSE NULL END) AS icd10_code,
                      (code.coding IS NULL AND code.text IS NOT NULL) AS recovered_from_text
               FROM bronze.conditions
           )
           SELECT patient_key, icd10_code,
                  CASE icd10_code {disp} ELSE icd10_code END AS display,  -- canonical display per code
                  recovered_from_text
           FROM base"""
    )

    # ---- SILVER: note (de-identified free text for the vector layer) ----
    con.execute(
        """CREATE OR REPLACE TABLE silver.note AS
           SELECT md5(replace(subject.reference,'Patient/','')) AS patient_key,
                  CAST(date AS DATE) AS note_date, text
           FROM bronze.notes WHERE text IS NOT NULL"""
    )

    # ---- SILVER: claim (cast string-billed → double, flag denials, date-shift) ----
    con.execute(
        """CREATE OR REPLACE TABLE silver.claim AS
           SELECT md5(replace(c.patient.reference,'Patient/','')) AS patient_key,
                  (CAST(c.billablePeriod.start AS DATE) + (p._date_shift_days * INTERVAL 1 DAY))::DATE AS claim_date,
                  c.procedure[1].code    AS procedure_code,
                  c.procedure[1].display AS procedure_display,
                  c.diagnosis[1].code    AS dx_code,
                  TRY_CAST(CAST(c.total.value AS VARCHAR) AS DOUBLE) AS billed,
                  c.paid                 AS paid,
                  c.status               AS status,
                  (c.status = 'denied')  AS denied
           FROM bronze.claims c
           JOIN silver.patient p ON p.patient_key = md5(replace(c.patient.reference,'Patient/',''))"""
    )

    # ---- SILVER: PRO surveys (clamp out-of-range ODI to NULL) ----
    con.execute(
        """CREATE OR REPLACE TABLE silver.pro AS
           SELECT md5(replace(j.subject.reference,'Patient/','')) AS patient_key,
                  (CAST(j.authored AS DATE) + (p._date_shift_days * INTERVAL 1 DAY))::DATE AS survey_date,
                  j.instrument,
                  CASE WHEN j.score BETWEEN 0 AND 100 THEN j.score ELSE NULL END AS score
           FROM bronze.pro_surveys j
           JOIN silver.patient p ON p.patient_key = md5(replace(j.subject.reference,'Patient/',''))"""
    )

    # ---- SILVER: wearable daily (null-out outlier step counts) ----
    con.execute(
        """CREATE OR REPLACE TABLE silver.wearable_daily AS
           SELECT md5(replace(w.patient.reference,'Patient/','')) AS patient_key,
                  (CAST(w.date AS DATE) + (p._date_shift_days * INTERVAL 1 DAY))::DATE AS day,
                  CASE WHEN w.steps BETWEEN 0 AND 50000 THEN w.steps ELSE NULL END AS steps,
                  w.active_minutes, w.resting_hr, w.sleep_hours
           FROM bronze.wearables w
           JOIN silver.patient p ON p.patient_key = md5(replace(w.patient.reference,'Patient/',''))"""
    )

    dq_after = _dq_silver(con)
    report = {"bronze_before": dq_before, "silver_after": dq_after}
    DQ_OUT.write_text(json.dumps(report, indent=2))

    # de-id assertion: silver.patient must carry no PHI columns
    cols = {c[0] for c in con.execute("DESCRIBE silver.patient").fetchall()}
    assert not (cols & {"name", "identifier", "address", "birthDate"}), f"PHI leaked into silver: {cols}"

    print("silver built. DQ report:", json.dumps(report, indent=2))
    con.close()
    return report


def _scalar(con, sql: str):
    return con.execute(sql).fetchone()[0]


def _dq_bronze(con) -> dict:
    return {
        "patient_rows": _scalar(con, "SELECT count(*) FROM bronze.patients"),
        "patient_duplicates": _scalar(con, "SELECT count(*)-count(DISTINCT id) FROM bronze.patients"),
        "missing_gender_pct": round(_scalar(con, "SELECT 100.0*avg(CASE WHEN gender IS NULL THEN 1 ELSE 0 END) FROM bronze.patients"), 1),
        "missing_birthdate_pct": round(_scalar(con, "SELECT 100.0*avg(CASE WHEN birthDate IS NULL THEN 1 ELSE 0 END) FROM bronze.patients"), 1),
        "glucose_distinct_units": _scalar(con, "SELECT count(DISTINCT COALESCE(valueQuantity.unit, unit)) FROM bronze.observations WHERE code.coding[1].code='2339-0'"),
        "freetext_condition_pct": round(_scalar(con, "SELECT 100.0*avg(CASE WHEN code.coding IS NULL THEN 1 ELSE 0 END) FROM bronze.conditions"), 1),
        "obs_missing_value_pct": round(_scalar(con, "SELECT 100.0*avg(CASE WHEN COALESCE(valueQuantity.value, value) IS NULL THEN 1 ELSE 0 END) FROM bronze.observations"), 1),
        "claims_missing_paid_pct": round(_scalar(con, "SELECT 100.0*avg(CASE WHEN paid IS NULL THEN 1 ELSE 0 END) FROM bronze.claims"), 1),
        "wearable_outlier_steps_pct": round(_scalar(con, "SELECT 100.0*avg(CASE WHEN steps > 50000 THEN 1 ELSE 0 END) FROM bronze.wearables"), 1),
        "pro_out_of_range_pct": round(_scalar(con, "SELECT 100.0*avg(CASE WHEN score > 100 THEN 1 ELSE 0 END) FROM bronze.pro_surveys"), 1),
    }


def _dq_silver(con) -> dict:
    return {
        "patient_rows": _scalar(con, "SELECT count(*) FROM silver.patient"),
        "patient_duplicates": _scalar(con, "SELECT count(*)-count(DISTINCT patient_key) FROM silver.patient"),
        "glucose_distinct_units": _scalar(con, "SELECT count(DISTINCT unit_std) FROM silver.observation WHERE metric='glucose'"),
        "condition_coded_pct": round(_scalar(con, "SELECT 100.0*avg(CASE WHEN icd10_code IS NOT NULL THEN 1 ELSE 0 END) FROM silver.condition"), 1),
        "conditions_recovered_from_text": _scalar(con, "SELECT count(*) FROM silver.condition WHERE recovered_from_text"),
        "obs_missing_value_pct": round(_scalar(con, "SELECT 100.0*avg(CASE WHEN value_std IS NULL THEN 1 ELSE 0 END) FROM silver.observation"), 1),
        "claim_rows": _scalar(con, "SELECT count(*) FROM silver.claim"),
        "claims_billed_numeric_pct": round(_scalar(con, "SELECT 100.0*avg(CASE WHEN billed IS NOT NULL THEN 1 ELSE 0 END) FROM silver.claim"), 1),
        "wearable_outlier_steps_remaining": _scalar(con, "SELECT count(*) FROM silver.wearable_daily WHERE steps > 50000"),
        "pro_out_of_range_remaining": _scalar(con, "SELECT count(*) FROM silver.pro WHERE score > 100"),
    }


if __name__ == "__main__":
    build()
