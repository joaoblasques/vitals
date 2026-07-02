# Roadmap

Built **MVP-first**: one source end-to-end before widening. Each phase leaves a working, demoable system.

## Phase 0 — Repo & tooling ✅
- [x] Repo, `mise` (databricks CLI / terraform / python) + `uv` venv
- [x] This documentation site live (auto-deployed via GitHub Actions)
- [x] Databricks Free Edition workspace; Unity Catalog + Delta schema (deployment target) — Terraform IaC in `infra/terraform/`, **applied & verified** on the live workspace (3 catalogs, 7 schemas, `landing` volume, live PHI-gating grants: analysts read silver+gold, never bronze)

## Phase 1 — MVP vertical slice (FHIR end-to-end) ✅
- [x] FHIR-shaped NDJSON landed in bronze (seeded synthetic generator)
- [x] Mess-injector (schema drift, dupes, unit drift, missingness — deterministic seed)
- [x] bronze→silver: flatten FHIR; de-id (PHI dropped + assertion); standardize LOINC/ICD-10, mmol/L→mg/dL
- [x] dbt silver→gold: `dim_patient`, `fct_observation`, `mart_condition_outcomes` metric mart
- [x] **29 dbt tests** on the silver/gold gate, passing; marts backed by a **MetricFlow semantic layer** (7 composable metrics, `make metrics-query`) ([ADR 0007](https://github.com/joaoblasques/vitals/blob/main/docs/adr/0007-dbt-semantic-layer.md))
- [x] Feast feature store (600×8) — **materialized offline→online (sqlite); point-in-time historical retrieval**, parity-proven vs offline parquet ([ADR 0008](https://github.com/joaoblasques/vitals/blob/main/docs/adr/0008-feast-feature-store.md))
- [x] Vector index + RAG query over clinical notes — real **pgvector** store (fastembed BGE-small 384-d, HNSW cosine, local Docker) with TF-IDF fallback when the store is absent ([ADR 0006](https://github.com/joaoblasques/vitals/blob/main/docs/adr/0006-pgvector-local-serving-store.md))
- [x] Demo surgery-risk model (MLflow, ROC-AUC 0.825 at Phase 1; the widened model scores ~0.75 — see [Results](results.md))
- [x] Single end-to-end run (`make run`) + Airflow DAG mirroring it
- See the [Results](results.md).

## Phase 2 — Widen sources + OMOP ✅
- [x] Land the **OMOP CDM** (person, condition_occurrence, measurement) with concept mapping + tests
- [x] Add claims (837/835) + PRO surveys + wearable batch as sources (cleaned at silver)
- [x] Expand marts & features — `mart_cost_outcomes`; 20-feature store across 4 sources

## Phase 3 — Streaming + scale ✅
- [x] Wearable stream via **Spark Structured Streaming** — real **Kafka source** (local Docker KRaft broker, `format("kafka")`), parity-proven identical (15169 events, file == kafka); file-source path remains the no-broker default ([ADR 0010](https://github.com/joaoblasques/vitals/blob/main/docs/adr/0010-kafka-streaming-source.md))
- [x] **PySpark-at-scale** transform with a window function (7-obs rolling pain per patient)

## Phase 4 — Governance & polish ✅
- [x] Lineage + data dictionary auto-generated from dbt → [Data Catalog](catalog.md)
- [x] Governance model (PHI classification, de-id boundary, Unity Catalog mapping) → [Governance](governance.md)
- [x] Drift monitoring (PSI) on the feature store
- [x] Decision records (ADRs) in repo `docs/adr/` + vault

### Original Phase 4 checklist
- [ ] Lineage + data dictionary + Unity Catalog governance; drift monitoring; decision write-ups

## Phase 5 — Deploy to Databricks (Delta-on-UC) ✅
Wire the local pipeline to write **Delta into Unity Catalog** on the live Free Edition workspace.
The full medallion (bronze → silver → gold) runs on Databricks with **end-to-end row-count + DQ
parity** verified against the local DuckDB pipeline.
- [x] **UC object graph applied & verified** via Terraform (`infra/terraform/`): 3 catalogs, 7 schemas, `landing` volume, live PHI-gating grants (analysts read silver+gold, never bronze). See ADR notes + the [Governance](governance.md) page.
- [x] **Bronze → Delta**: backend abstraction (`VITALS_TARGET=local|databricks`); raw NDJSON landed into the `vitals_bronze.raw.landing` UC volume and written as 8 Delta tables in `vitals_bronze.raw.*` via databricks-connect on serverless ([ADR 0005](https://github.com/joaoblasques/vitals/blob/main/docs/adr/0005-spark-execution-databricks-connect.md)). Row-count parity vs the local DuckDB bronze verified for all 8 sources.
- [x] **Silver → Delta** (the PHI boundary on UC): de-id + conform ported to Spark, written as 8 Delta tables in `vitals_silver.clinical.*`. De-id assertion (no identifiers in `silver.patient`) + full DQ parity vs local DuckDB verified (row counts, coding %, unit standardization, text-recovery). Cross-engine verification also surfaced & fixed a latent DuckDB bug (quoted-JSON billed amounts silently dropped).
- [x] **Gold via dbt-databricks**: the existing dbt models build into `vitals_gold.marts` on the serverless SQL warehouse (target-aware `silver` source resolves to `vitals_silver.clinical`). All 10 models + 26 dbt tests pass on Databricks; all 11 gold tables match local DuckDB row counts.
- [x] **Production deploy path** (the deployment half of [ADR 0005](https://github.com/joaoblasques/vitals/blob/main/docs/adr/0005-spark-execution-databricks-connect.md)): a **Databricks Asset Bundle** (`databricks.yml`) ships the gold stage as a **scheduled serverless job** (dbt marts + the 26 dbt tests as in-job quality gates), deployed + run from code (`make bundle-deploy` / `bundle-run`, verified `TERMINATED SUCCESS`). Promoting bronze/silver into the job (a `python_wheel_task`) is the documented next step.

## Phase 6 — three-store gold made real, governed & streamed ✅
- [x] **Full-medallion `python_wheel_task` job**: a single scheduled serverless run does generate → bronze → silver → gold → drift, no laptop; verified **TERMINATED SUCCESS** ([ADR 0005 Update](https://github.com/joaoblasques/vitals/blob/main/docs/adr/0005-spark-execution-databricks-connect.md)). Includes failure alerts (`on_failure` email, address injected at deploy time) and drift monitoring as a downstream job task writing to `vitals_gold.monitoring.drift_report`.
- [x] **MetricFlow semantic layer** over the marts: 7 composable metrics declared in YAML, parity-proven vs the dbt marts, queryable via `make metrics-query` ([ADR 0007](https://github.com/joaoblasques/vitals/blob/main/docs/adr/0007-dbt-semantic-layer.md)).
- [x] **Great Expectations** gates the silver DQ contract in CI: coded-vocabulary value-sets (every `icd10_code` ∈ ICD-10, `observation.metric` ∈ standard set, glucose `unit_std` == `mg/dL`), PHI boundary column check, ranges + key uniqueness (`make dq`) ([ADR 0009](https://github.com/joaoblasques/vitals/blob/main/docs/adr/0009-great-expectations-silver-dq.md)).
- [x] **Hermetic CI quality gate** (`.github/workflows/ci.yml`): ruff + unit tests + full local pipeline + the GE silver gate, on every push — DQ can't be skipped.
