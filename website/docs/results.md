# Results

The MVP vertical slice runs end-to-end (`make run`) and produces these real, reproducible
outputs from 600 synthetic patients. All numbers are deterministic (seeded).

## 1. Data quality — bronze (messy) → silver (trusted)

The silver layer earns its keep. Before/after on the same data:

| Dimension | Bronze (raw) | Silver (clean) |
|---|---|---|
| Patient rows | 631 | **600** (31 exact duplicates removed) |
| Glucose units in use | **2** (mg/dL *and* mmol/L) | **1** (standardized to mg/dL) |
| Conditions coded to ICD-10 | 79.5% | **100%** (123 recovered from free text) |
| Observations missing a value | 3.9% | **0%** (completeness gate) |
| Missing gender / birthdate | 9.2% / 4.8% | handled (PHI removed; age capped at 90) |

PHI (names, SSNs, addresses, full DOBs) is dropped at the silver boundary — a de-id assertion in
the pipeline fails the build if any PHI column survives. Dates are shifted per-patient to preserve
intervals (HIPAA Safe Harbor).

## 2. Analytics mart — `gold.mart_condition_outcomes`

Per primary condition: cohort size, surgery rate, mean pain, mean adherence (built in dbt, tested).

| Condition | ICD-10 | Patients | Surgery rate | Avg pain | Avg adherence % |
|---|---|---:|---:|---:|---:|
| Lumbar disc displacement | M51.26 | 132 | 0.333 | 4.74 | 52.1 |
| Knee osteoarthritis (bilateral) | M17.0 | 125 | 0.304 | 4.79 | 55.6 |
| Low back pain | M54.5 | 132 | 0.068 | 4.55 | 56.5 |
| Pain in right knee | M25.561 | 104 | 0.067 | 4.57 | 56.2 |
| Rotator cuff tear | M75.100 | 107 | 0.065 | 4.93 | 50.7 |

dbt build: **3 models + 8 data tests, all passing** (uniqueness, not-null, accepted-values, referential integrity).

## 3. Feature store — `gold.patient_features`

600 patients × time-aware features (offline table + Parquet; Feast repo in `ml/feature_store/`):
`age, mean_pain, last_pain, pain_trend, mean_adherence, mean_glucose_mgdl, mean_hr, n_observations`.

## 4. Vector index + RAG

399 clinical notes embedded (TF-IDF in the MVP; **pgvector + clinical embeddings** in production).

> **Query:** *"severe lower back pain worse with sitting, poor adherence"*
> **Top match (0.69):** *"Patient reports severe lower back pain, worse with prolonged sitting.
> Adherence to home program poor adherence. Plan: continue PT, reassess in 4 weeks."*

## 5. Demo model — surgery-risk (tracked in MLflow)

Logistic regression on the feature store, predicting `surgery_within_90d`:

| Metric | Value |
|---|---|
| ROC-AUC | **0.825** |
| Accuracy | 0.847 |
| Train / test | 450 / 150 |
| Positive rate | 17.5% |

The learned coefficients are clinically coherent — **mean pain (+1.11)** and **age (+0.85)** raise
risk, while **adherence (−0.62)** lowers it — exactly the relationship Sword's care model is built on.

## 6. OMOP CDM (Phase 2)

Silver is also conformed to the **OMOP Common Data Model** — the standard health-data analysts and
researchers recognize — in dbt, with source codes mapped to standard concepts:

| OMOP table | Rows | Notes |
|---|---:|---|
| `omop_person` | 600 | standard gender concepts (8507 / 8532) |
| `omop_condition_occurrence` | 600 | ICD-10 → standard condition concept (e.g. M54.5 → 194133 "Low back pain") |
| `omop_measurement` | 5,303 | LOINC → standard measurement concept (e.g. 2339-0 → 3004501 "Glucose") |

The concept mapping is a dbt seed (illustrative concept IDs); in production it's loaded from the
full OHDSI Athena vocabulary. Referential integrity (`person_id` FKs) is enforced by dbt tests.

---

*Reproduce:* `make setup && make run` → writes `data/results.json`, the DuckDB lakehouse, and an
MLflow run. See the [Dev Log](dev-log.md) for the build narrative.
