# Governance & Monitoring

How Vitals keeps data **trusted**: a clear PHI boundary, column classification, lineage, and drift
monitoring. The MVP enforces these locally; the right column shows the production (Databricks Unity
Catalog) equivalent.

## PHI & de-identification

PHI is present in **bronze** and removed at the **silver** boundary — everything downstream
(analytics, features, ML, vectors) reads only de-identified data. A build-time assertion fails the
pipeline if any PHI column survives into silver.

| Classification | Examples | Handling |
|---|---|---|
| **Direct identifier (PHI)** | name, SSN, address, full DOB | dropped at silver (HIPAA Safe Harbor — 18 identifiers) |
| **Quasi-identifier** | date of service, age | dates shifted per-patient (intervals preserved); age capped at 90 |
| **Surrogate key** | `patient_key` | salted hash of the source id |
| **Clinical (safe)** | conditions, observations, scores | retained, standardized to ICD-10/LOINC/OMOP |

| MVP (here) | Production (Databricks) |
|---|---|
| de-id assertion in the pipeline | Unity Catalog column masks + row filters |
| `patient_key` hashing | UC governed surrogate + access policies |
| schema separation `bronze`/`silver`/`gold` | UC catalogs/schemas with grants per tier (provisioned by `infra/terraform/`) |

## Lineage & data dictionary

End-to-end lineage and a per-column dictionary are **auto-generated from dbt** — see
[Data Catalog & Lineage](catalog.md). It regenerates from `manifest.json` + `catalog.json` whenever
the models change, so documentation can't drift from the code. In production this is Unity Catalog's
automatic table/column lineage.

## Drift monitoring

Healthcare distributions shift (new cohorts, seasonality, devices), silently degrading models. A
**Population Stability Index (PSI)** monitor compares a reference window to the current window per
feature (`python -m vitals.monitoring`). Bands: <0.1 stable · 0.1–0.2 moderate · >0.2 significant.

Demonstrated two ways:

| Scenario | Result |
|---|---|
| **Natural split** (reference vs held-out current) | all features **stable** (one moderate) — no false alarms |
| **Injected population shift** (sicker, less active cohort) | **flags `mean_pain`, `mean_odi`, `mean_active_min` as significant** + `mean_steps` moderate |

In production this runs on a schedule and alerts when a serving feature drifts past threshold,
triggering retraining review — essential where a stale model affects care.

## Decisions

The non-obvious engineering choices (DuckDB-vs-Databricks, de-identification, FHIR→OMOP, the
three-store gold) are recorded as ADRs in the repo under `docs/adr/`.
