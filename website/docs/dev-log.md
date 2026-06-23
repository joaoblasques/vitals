# Dev Log

A running, dated log of what was built and what was learned — newest first.

## 2026-06-23 — Project kickoff
- Defined the project: a health-data medallion lakehouse producing analytics marts + a feature
  store + a vector index ("three-store gold layer").
- Locked the architecture (medallion + healthcare layer: FHIR→OMOP, de-id at silver,
  coded-vocabulary data quality) and the stack (Databricks/Delta, dbt, Airflow, PySpark, Feast,
  pgvector, MLflow).
- Scaffolded the repo, tooling (mise + uv), and this documentation site.
- Next: Phase 1 MVP vertical slice — Synthea FHIR through bronze→silver→gold to a first feature and
  a first vector index.
