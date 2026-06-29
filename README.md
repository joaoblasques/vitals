# Vitals

**From raw clinical signals to trusted, AI-ready data.**

[![CI](https://github.com/joaoblasques/vitals/actions/workflows/ci.yml/badge.svg)](https://github.com/joaoblasques/vitals/actions/workflows/ci.yml)

A governed **medallion lakehouse** that ingests messy, multi-source health data and turns it into
three trusted outputs: **analytics marts**, an **ML feature store**, and a **vector index** for
retrieval — built on Databricks/Delta with dbt, Airflow, and PySpark.

> 🌐 **Website / docs:** https://joaoblasques.com/vitals/
> 📋 Status: **in active development** — see the [Roadmap](https://joaoblasques.com/vitals/roadmap/).

---

## Why this exists

Healthcare data is the messiest data there is: many source systems, competing vocabularies,
silent unit drift, PHI everywhere, and free text where codes belong. The job of a data engineer is
to turn that into datasets the business — and increasingly, AI models — can *trust*.

**Vitals** is a reference implementation of exactly that: raw FHIR/claims/wearable/notes data in,
clean and governed data out, served three ways for the three things a modern health-tech company
actually does with data.

## The three-store gold layer

One pipeline, three consumption shapes — because analytics, classical ML, and LLM/RAG need
different things:

| Output | Tech | Serves |
|---|---|---|
| **Analytics marts** | dbt (Kimball star) + semantic layer | BI, cohorts, clinical/commercial reporting |
| **Feature store** | Feast (offline + online) | surgery-risk / adherence ML models |
| **Vector index** | pgvector | RAG / semantic search over clinical notes |

## Architecture (medallion + a healthcare layer)

```
BRONZE  raw & messy: FHIR (Synthea) · claims · wearable sensor streams · PRO surveys · notes
        (schema drift, dupes, mixed units, missingness, PHI present & gated)
   ↓
SILVER  de-identified (HIPAA Safe Harbor + date-shift) = PHI boundary; FHIR flattened;
        codes standardized (ICD-10/SNOMED/LOINC/RxNorm); OMOP CDM; data-quality contracts
   ↓
GOLD    analytics marts  +  feature store  +  vector index
   ↓
PROVE   MLflow demo model (features) + a RAG demo (vectors)
```

The **FHIR→OMOP, de-identification at silver, and coded-vocabulary data quality** are what set this
apart from a generic ETL project.

## Tech stack

Databricks (Delta + Unity Catalog) · Python + SQL · **dbt** · **Airflow** · **PySpark** + Spark
Structured Streaming · **Kafka** · **Feast** · **pgvector** · **MLflow** · **Great Expectations** ·
**Terraform**. Tooling pinned with **mise**; Python deps via **uv**.

## Repository layout

```
pipelines/   bronze · silver · gold (PySpark)
dbt/         silver→gold transformations, tests, semantic layer
airflow/     orchestration DAGs
ml/          Feast feature store + MLflow demos + pgvector RAG
data/        Synthea output + mess-injector (gitignored)
website/     MkDocs Material site (the public docs)
docs/        concept & decision docs
```

## Quickstart

Runs end-to-end locally on DuckDB — no cloud or Java needed:

```bash
make setup     # uv venv + install the runnable stack
make run       # generate → bronze → silver → dbt gold → features + vectors + model
```

Outputs: a DuckDB lakehouse (`data/vitals.duckdb`, schemas `bronze`/`silver`/`gold`), a
data-quality report (`data/dq_report.json`), a feature Parquet, an MLflow run, and
`data/results.json`. See the [Results](https://joaoblasques.com/vitals/results/) for what it produces.

> **Why DuckDB?** The MVP is built to *run* in one command. **Databricks/Delta** is the documented
> deployment target and **PySpark** the Phase-3 scale path (`pip install -e '.[databricks]'`).

## Status & roadmap

This is a portfolio project under active construction. The plan is public and tracked on the
[Roadmap](https://joaoblasques.com/vitals/roadmap/) — built MVP-first (one source end-to-end
before widening).

## License

MIT © João Blasques
