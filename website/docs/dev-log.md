# Dev Log

A running, dated log of what was built and what was learned — newest first.

## 2026-07-02 — Real Kafka stream source ✅
- Wearable stream now reads `format("kafka")` from a **local Docker KRaft broker** (single-node, no Zookeeper). A `kafka-python` producer publishes 15169 events to the `wearables` topic; the Spark consumer runs the **same** `clean_wearables` transform → same parquet sink as the file path.
- `make stream-parity` confirms cleaned output is **identical** (15169 events, file == kafka) — concrete proof the source swap changes nothing downstream. Closes the "compatible but not exercised" gap. ([ADR 0010](https://github.com/joaoblasques/vitals/blob/main/docs/adr/0010-kafka-streaming-source.md))

## 2026-07-01 — Great Expectations silver DQ gate ✅
- **Great Expectations** (GX Core 1.x) is now the **gating** DQ contract for silver. A code-defined suite (`src/vitals/dq.py`) validates coded-vocabulary value-sets (every `icd10_code` ∈ the ICD-10 set, `observation.metric` ∈ the standard set, glucose `unit_std` == `mg/dL`), the PHI boundary column set, and ranges + key uniqueness — exits non-zero on any violation.
- **CI runs it after `make build`**: the gate can't be skipped. Complements (not replaces) the descriptive `dq_report.json`; dbt tests still gate gold. ([ADR 0009](https://github.com/joaoblasques/vitals/blob/main/docs/adr/0009-great-expectations-silver-dq.md))

## 2026-06-30 — Feast feature store made real ✅
- Feast was scaffolded but never applied. Now **materialized offline→online (sqlite)**: `get_online_features` (low-latency inference path) + **point-in-time historical retrieval** (`get_historical_features` over an entity dataframe — the leakage-safe training join). Both paths parity-checked against the offline parquet (NULL-aware, float-tolerant). `make feast-demo`.
- Production would point the offline store at Databricks/Delta — noted, not exercised (local is the deliverable). ([ADR 0008](https://github.com/joaoblasques/vitals/blob/main/docs/adr/0008-feast-feature-store.md))

## 2026-06-30 — Full-medallion job on Databricks ✅
- Bronze + silver are now **in the Asset Bundle job** as a `python_wheel_task` (`medallion_ingest`). One scheduled serverless run does generate → bronze Delta → silver Delta → gold (dbt + 29 tests) → drift monitor, no laptop. Verified **TERMINATED SUCCESS**: `medallion_ingest` (bronze=28816, silver=27402) → `gold_dbt` → `drift_monitor`. ([ADR 0005 Update](https://github.com/joaoblasques/vitals/blob/main/docs/adr/0005-spark-execution-databricks-connect.md))
- Three Free-Edition lessons learned live: ship a lean wheel (core deps → `local` extra); pin the compute Python version (env version 3 / Python 3.12); branch dbt dialect on `target.type` (`metricflow_time_spine` needed `range()` on DuckDB vs `sequence()+explode()` on Spark).

## 2026-06-29 — dbt semantic layer + real pgvector RAG ✅
- **MetricFlow semantic layer**: 7 composable metrics (`surgery_rate`, `avg_conservative_spend`, …) declared in YAML over a new `fct_patient_metrics` per-patient base. `mf query` results parity-proven against the marts; `make metrics-query`. ([ADR 0007](https://github.com/joaoblasques/vitals/blob/main/docs/adr/0007-dbt-semantic-layer.md))
- **pgvector** replaces the TF-IDF placeholder: Docker (`pgvector/pgvector:pg16`), fastembed `bge-small-en-v1.5` (384-d ONNX/CPU, no API keys), **HNSW cosine** index, idempotent upsert. TF-IDF path remains the fallback when the store or `vector` extra is absent. `make rag-up / rag-index / rag-query`. ([ADR 0006](https://github.com/joaoblasques/vitals/blob/main/docs/adr/0006-pgvector-local-serving-store.md))

## 2026-06-26 — Databricks deploy path + ops hardening ✅
- **Asset Bundle job** (`databricks.yml`) ships gold as a scheduled serverless job (`make bundle-deploy` / `bundle-run`, verified `TERMINATED SUCCESS`). Dev path stays databricks-connect for fast iteration; the bundle path is the "how this ships in a real shop" answer — two modes, one codebase behind a target switch.
- **Failure alerts** (`on_failure` email, address injected at deploy time — no address committed to this public repo) and **drift monitoring as a job task** (`drift_monitor` `spark_python_task` runs downstream of `gold_dbt`, scores PSI feature-drift on every run, appends to `vitals_gold.monitoring.drift_report`).
- **Hermetic CI gate** (`.github/workflows/ci.yml`): ruff + unit tests + full local pipeline + GE silver gate, on every push. ([ADR 0005](https://github.com/joaoblasques/vitals/blob/main/docs/adr/0005-spark-execution-databricks-connect.md))

## 2026-06-23 — Phase 4: governance & polish ✅
- **Drift monitoring** (`monitoring.py`): PSI per feature, reference vs current. Stable on a natural
  split; correctly **flags an injected population shift** (pain/ODI/activity → significant).
- **Auto-generated data dictionary + lineage** (`catalog.py`) from dbt's manifest/catalog — a
  Mermaid lineage graph (10 models, 17 edges) + per-column dictionary that can't drift from the code.
- **Governance page**: PHI classification, the silver de-id boundary, and the Unity Catalog
  production mapping.
- **ADRs** (`docs/adr/`) for the four non-obvious decisions (DuckDB-vs-Databricks, de-id, OMOP,
  three-store gold); summarized in the vault.

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
