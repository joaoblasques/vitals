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

600 patients × **20 time-aware features spanning four source types** (offline table + Parquet;
Feast repo in `ml/feature_store/`):

- **observations** — mean/last/trend pain, adherence, glucose, heart rate
- **claims** — claim count, conservative-care spend, had-imaging, denial rate
- **PRO** — Oswestry Disability Index (mean + latest)
- **wearables** — mean steps, active minutes, resting HR, sleep

The store holds all 20; the demo model below uses a curated, clinically-relevant subset (feature selection).

## 4. Vector index + RAG

399 clinical notes embedded (TF-IDF in the MVP; **pgvector + clinical embeddings** in production).

> **Query:** *"severe lower back pain worse with sitting, poor adherence"*
> **Top match (0.69):** *"Patient reports severe lower back pain, worse with prolonged sitting.
> Adherence to home program poor adherence. Plan: continue PT, reassess in 4 weeks."*

## 5. Demo model — surgery-risk (tracked in MLflow)

Logistic regression on a curated 10-feature subset of the store, predicting `surgery_within_90d`:

| Metric | Value |
|---|---|
| ROC-AUC | **0.748** |
| Accuracy | 0.84 |
| Train / test | 450 / 150 |
| Model features | 10 (curated from 20) |

The learned coefficients are clinically coherent — **disability (ODI +0.72)**, **age (+0.64)**, and
**pain** raise risk, while **activity (active minutes −0.74, steps)** lowers it — exactly the
relationship Sword's care model is built on. Multi-source features (claims imaging, ODI, wearables)
now sit among the top predictors.

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

## 7. Multi-source ingestion (Phase 2)

Three more source types now flow through bronze → silver → dbt gold, each cleaned at the silver gate:

| Source | Bronze mess | Silver fix |
|---|---|---|
| **Claims** (837/835-style, 1,510 rows) | 9.7% missing paid; ~5% billed as string | string→numeric (96% recovered); denials flagged |
| **PRO surveys** (ODI, 1,718 rows) | 1.9% scores out of range (>100) | clamped → **0 remaining** |
| **Wearables** (daily batch, 15,169 rows) | 3.0% outlier step counts | nulled → **0 remaining** |

### Cost mart — `gold.mart_cost_outcomes`

A value-based-care view (the kind Sword reports to payers): conservative-care spend, imaging rate,
and surgery rate per condition.

| Condition | Patients | Surgery rate | Avg conservative spend | Imaging rate |
|---|---:|---:|---:|---:|
| Lumbar disc displacement | 129 | 0.341 | $1,004 | 70.5% |
| Low back pain | 115 | 0.035 | $890 | 59.1% |
| Pain in right knee | 116 | 0.017 | $816 | 60.3% |
| Knee osteoarthritis (bilateral) | 124 | 0.282 | $784 | 54.0% |
| Rotator cuff tear | 116 | 0.052 | $729 | 62.1% |

dbt now totals **1 seed + 10 models + 26 data tests, all passing**.

---

*Reproduce:* `make setup && make run` → writes `data/results.json`, the DuckDB lakehouse, and an
MLflow run. See the [Dev Log](dev-log.md) for the build narrative.
