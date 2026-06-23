# Glossary

Every term used across the site, one line each.

## Architecture

| Term | Meaning |
|---|---|
| **Medallion** | A design with three quality tiers — **bronze** (raw), **silver** (cleaned/conformed), **gold** (consumption-ready). A lifecycle, not a schema. |
| **Bronze** | Raw ingested data, as-is; schema drift tolerated; PHI present & access-gated. |
| **Silver** | De-identified, type-clean, standardized. The **PHI boundary** — everything downstream reads only from here. |
| **Gold** | Consumption-shaped outputs: analytics marts, feature store, vector index. |
| **Delta / Iceberg** | Open table formats giving object storage ACID transactions, schema evolution, time travel. |
| **Unity Catalog** | Databricks governance layer: catalogs/schemas, table/column access, automatic lineage. |
| **Lineage** | The dependency graph from sources → models → outputs (auto-generated from dbt here). |

## Healthcare data

| Term | Meaning |
|---|---|
| **FHIR** | HL7's standard for clinical data as JSON *Resources* (Patient, Encounter, Condition, Observation…). Often exported as NDJSON. |
| **OMOP CDM** | OHDSI's Common Data Model — one standard format + vocabulary so disparate sources analyze the same way. |
| **Concept id** | OMOP's standard integer code for a clinical concept (e.g. gender 8507 = male); loaded from OHDSI Athena. |
| **ICD-10 / LOINC / SNOMED / RxNorm** | Vocabularies for diagnoses / labs & observations / clinical findings / medications. |
| **PHI** | Protected Health Information — names, SSN, address, full dates, etc. |
| **Safe Harbor** | HIPAA de-identification route: remove 18 specified identifier types. |
| **Date-shift** | Shift each patient's dates by a fixed per-patient offset — preserves intervals while de-identifying. |
| **patient_key** | Salted-hash surrogate replacing the real patient id. |

## Standards, organizations & acronyms

| Term | Meaning |
|---|---|
| **HL7** | Health Level Seven International — the standards body that publishes FHIR (and older HL7 v2 messaging). |
| **HIPAA** | US health-privacy law. Defines PHI and the two de-identification routes. |
| **Expert Determination** | HIPAA de-id route #2: a qualified statistician certifies re-identification risk is "very small". |
| **De-identification** | Removing PHI so data can be used for analytics/ML. In Vitals: done at the silver boundary. |
| **OHDSI** ("Odyssey") | Observational Health Data Sciences and Informatics — the open community maintaining OMOP, its tools, and vocabularies. |
| **Athena** | OHDSI's vocabulary repository — where standard OMOP concept_ids are downloaded from. |
| **Interoperability** | Different systems exchanging data they can both use — FHIR's purpose. |
| **NDJSON** | Newline-delimited JSON — one object per line; the FHIR bulk-export format. |
| **EHR** | Electronic Health Record — the clinical source system. |
| **Claims (837 / 835)** | Billing data: 837 = claim submitted to the payer; 835 = the payer's remittance/payment. |
| **CPT** | Current Procedural Terminology — procedure codes in claims (MRI, physical therapy, injection…). |
| **PRO** | Patient-Reported Outcome — a survey the patient fills in (e.g. pain, disability). |
| **ODI** | Oswestry Disability Index — the PRO instrument used here (0–100; higher = worse). |
| **Synthea** | Open-source synthetic FHIR patient generator — PHI-free data the project can publish. |
| **Value-based care** | Paying for outcomes, not volume — the cost mart reflects it. |

## Transformation & serving

| Term | Meaning |
|---|---|
| **dbt** | SQL transformation framework: versioned models, tests, docs, lineage. Builds silver→gold here. |
| **Dimensional model** | Kimball star schema: `dim_` (entities) + `fct_` (events/measurements). |
| **Semantic layer / metric** | Governed, reusable metric definitions analysts consume (e.g. surgery rate). |
| **Feature store** | Serves ML features consistently for training (offline) and scoring (online). Here: Feast. |
| **Vector index** | Embeddings of text for similarity search / RAG. Here: TF-IDF (pgvector in prod). |
| **RAG** | Retrieval-Augmented Generation: fetch relevant text, feed it to an LLM. |
| **MLflow** | Experiment tracking + model registry. |

## ML & reliability

| Term | Meaning |
|---|---|
| **ROC-AUC** | Ranking quality of a classifier (0.5 = random, 1.0 = perfect). |
| **Label leakage** | A feature that encodes the outcome — inflates offline scores, fails in production. Avoided by using only conservative-care claims. |
| **Feature selection** | Using a curated subset of available features in a model (store stays rich, model stays clean). |
| **PSI** | Population Stability Index — drift metric per feature: <0.1 stable, 0.1–0.2 moderate, >0.2 significant. |
| **Structured Streaming** | Spark's streaming engine; same API as batch. Uses trigger, checkpoint, watermark. |
| **Window function** | Per-partition ordered computation (e.g. 7-row rolling mean per patient). |

## Tooling

| Term | Meaning |
|---|---|
| **DuckDB** | In-process single-file analytical DB. Runs the MVP with no cloud (Databricks is the deploy target). |
| **mise / uv** | Pin tool versions / manage Python deps fast. |
