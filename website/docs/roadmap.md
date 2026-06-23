# Roadmap

Built **MVP-first**: one source end-to-end before widening. Each phase leaves a working, demoable system.

## Phase 0 — Repo & tooling
- [ ] Repo, `mise` (databricks CLI / terraform / python) + `uv` venv
- [ ] Databricks Free Edition workspace; Unity Catalog + Delta schema
- [ ] This documentation site live

## Phase 1 — MVP vertical slice (FHIR end-to-end) ⭐
- [ ] Synthea → FHIR NDJSON landed in bronze (Delta)
- [ ] Mess-injector (schema drift, dupes, unit drift, missingness — deterministic seed)
- [ ] PySpark bronze→silver: flatten Patient/Encounter/Condition/Observation; de-id; standardize LOINC/ICD-10
- [ ] dbt silver→gold: `dim_patient`, `fct_observation`, one semantic-layer metric
- [ ] dbt tests + Great Expectations on the silver gate
- [ ] One Feast feature table
- [ ] One pgvector index over synthetic clinical notes
- [ ] One demo: adherence/surgery-risk model (MLflow) + a RAG query
- [ ] Airflow DAG orchestrating the slice

## Phase 2 — Widen sources + OMOP
- [ ] Claims + PRO surveys + wearable batch; land OMOP CDM; expand marts & features

## Phase 3 — Streaming + scale
- [ ] Kafka → Spark Structured Streaming for wearable sensors; PySpark-at-scale pass

## Phase 4 — Governance & polish
- [ ] Lineage + data dictionary + Unity Catalog governance; drift monitoring; decision write-ups
