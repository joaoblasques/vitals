# Dev Log

A running, dated log of what was built and what was learned — newest first.

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
