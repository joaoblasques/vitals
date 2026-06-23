# Dev Log

A running, dated log of what was built and what was learned — newest first.

## 2026-06-23 — Phase 3: streaming + Spark at scale ✅
- Wearables now also flow through a **Spark Structured Streaming** job: file source → cleaned
  Parquet sink with checkpointing, `trigger(availableNow)`. 15,169 events streamed, **448 outliers
  nulled on the fly**. Production swaps the source to **Kafka** — one line, identical downstream.
- Added a **PySpark-at-scale** batch transform with a **window function** (7-obs rolling pain per
  patient) — the Databricks scale path for the silver logic (1,631 rows).
- Infra note: Spark 4 needs JDK 17/21 (not 24); the modules auto-select an installed 17/21 JDK.

## 2026-06-23 — Phase 2: multi-source ingestion ✅
- Added three source types through bronze→silver→dbt gold: **claims** (837/835-style, 1,510),
  **PRO surveys** (Oswestry Disability Index, 1,718), **wearables** (daily batch, 15,169).
- Each with its own injected mess + silver fix: billed-as-string → numeric (96% recovered),
  out-of-range ODI clamped (0 remaining), outlier step counts nulled (0 remaining).
- New dbt models: `fct_claim`, `fct_pro`, `fct_wearable_daily`, and `mart_cost_outcomes` (a
  value-based-care view: conservative spend, imaging rate, surgery rate per condition).
- **Leakage guard:** claims contain only conservative-care CPTs (office, MRI, PT, injection) — no
  surgery codes — so they predict the future outcome without leaking it.
- Feature store grew to **20 features across 4 sources**; demo model uses a curated 10
  (feature selection). dbt now **1 seed + 10 models + 26 tests, all passing**.

## 2026-06-23 — Phase 2 begins: OMOP CDM ✅
- Conformed silver into the **OMOP Common Data Model** in dbt: `omop_person`,
  `omop_condition_occurrence`, `omop_measurement` (600 / 600 / 5,303 rows).
- Source codes mapped to standard concepts via a dbt seed (`concept_map.csv`): ICD-10 → condition
  concepts, LOINC → measurement concepts, gender → 8507/8532. Referential integrity tested.
- dbt now: **1 seed + 6 models + 18 tests, all passing.**
- Next: widen sources (claims 837/835, PRO surveys, wearable batch) and expand features.

## 2026-06-23 — Phase 1 MVP slice: working end-to-end ✅
- Built the full vertical slice: **generate → bronze → silver → dbt gold → serve**, runnable with
  one command (`make run`). See [Results](results.md) for the real numbers.
- **Bronze**: a seeded generator emits FHIR-shaped NDJSON with *deliberate* mess (dupe patients,
  mixed glucose units, free-text conditions, schema drift, missing values) — so the cleaning layer
  has real work to show.
- **Silver**: de-identify (PHI dropped at the boundary, with a build-failing assertion), flatten
  FHIR, standardize glucose mmol/L→mg/dL, recover ICD-10 from free text (123 conditions), dedupe.
  Data-quality report written to `data/dq_report.json`.
- **Gold (dbt)**: `dim_patient`, `fct_observation`, `mart_condition_outcomes` — 3 models + 8 tests,
  all passing.
- **Serve**: a 600×8 feature store (offline table + Parquet + Feast repo), a TF-IDF vector index
  with a working RAG query, and a surgery-risk model (**ROC-AUC 0.825**) tracked in MLflow.
- **Engineering decision**: the MVP runs on **DuckDB** for one-command reproducibility; **Databricks
  /Delta** is the documented deployment target and **PySpark** the Phase-3 scale path. Runnable > impressive-but-dead.
- Next (Phase 2): widen sources (claims, PRO, wearable batch) and land the OMOP CDM.

## 2026-06-23 — Project kickoff
- Defined the project, locked the architecture (medallion + healthcare layer), scaffolded the repo,
  tooling (mise + uv), and this documentation site.
