# Roadmap

Built **MVP-first**: one source end-to-end before widening. Each phase leaves a working, demoable system.

## Phase 0 — Repo & tooling ✅
- [x] Repo, `mise` (databricks CLI / terraform / python) + `uv` venv
- [x] This documentation site live (auto-deployed via GitHub Actions)
- [x] Databricks Free Edition workspace; Unity Catalog + Delta schema (deployment target) — Terraform IaC in `infra/terraform/`

## Phase 1 — MVP vertical slice (FHIR end-to-end) ✅
- [x] FHIR-shaped NDJSON landed in bronze (seeded synthetic generator)
- [x] Mess-injector (schema drift, dupes, unit drift, missingness — deterministic seed)
- [x] bronze→silver: flatten FHIR; de-id (PHI dropped + assertion); standardize LOINC/ICD-10, mmol/L→mg/dL
- [x] dbt silver→gold: `dim_patient`, `fct_observation`, `mart_condition_outcomes` metric mart
- [x] dbt tests on the silver/gold gate (8 tests passing)
- [x] Feast feature table (600×8, offline Parquet + Feast repo)
- [x] Vector index + RAG query over clinical notes (TF-IDF; pgvector is the prod target)
- [x] Demo surgery-risk model (MLflow, ROC-AUC 0.825)
- [x] Single end-to-end run (`make run`) + Airflow DAG mirroring it
- See the [Results](results.md).

## Phase 2 — Widen sources + OMOP ✅
- [x] Land the **OMOP CDM** (person, condition_occurrence, measurement) with concept mapping + tests
- [x] Add claims (837/835) + PRO surveys + wearable batch as sources (cleaned at silver)
- [x] Expand marts & features — `mart_cost_outcomes`; 20-feature store across 4 sources

## Phase 3 — Streaming + scale ✅
- [x] Wearable stream via **Spark Structured Streaming** (file-source demo, checkpointed sink; Kafka in prod)
- [x] **PySpark-at-scale** transform with a window function (7-obs rolling pain per patient)

## Phase 4 — Governance & polish ✅
- [x] Lineage + data dictionary auto-generated from dbt → [Data Catalog](catalog.md)
- [x] Governance model (PHI classification, de-id boundary, Unity Catalog mapping) → [Governance](governance.md)
- [x] Drift monitoring (PSI) on the feature store
- [x] Decision records (ADRs) in repo `docs/adr/` + vault

### Original Phase 4 checklist
- [ ] Lineage + data dictionary + Unity Catalog governance; drift monitoring; decision write-ups
