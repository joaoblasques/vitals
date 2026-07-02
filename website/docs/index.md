# Vitals

**From raw clinical signals to trusted, AI-ready data.**

Vitals is a governed **medallion lakehouse** that ingests messy, multi-source health data and turns
it into three trusted outputs — **analytics marts**, an **ML feature store**, and a **vector index**
for retrieval. It's a reference implementation of the work a modern health-tech data team does every
day: turn chaos into datasets the business *and* its AI models can trust.

!!! info "Status"
    In active development, built **MVP-first**. Follow progress on the [Roadmap](roadmap.md) and
    [Dev Log](dev-log.md).

## The problem

Healthcare data is the messiest data there is: many source systems, competing vocabularies, silent
unit drift (mg/dL ↔ mmol/L), PHI everywhere, and free text where coded values belong. Left unmodeled,
it produces silently-biased analytics and unreliable models.

## The approach

One pipeline, three consumption shapes — because analytics, classical ML, and LLM/RAG each need
something different:

| Output | Tech | Serves |
|---|---|---|
| **Analytics marts** | dbt (Kimball star) + semantic layer | BI, cohorts, reporting |
| **Feature store** | Feast (offline + online) | surgery-risk / adherence models |
| **Vector index** | pgvector | RAG over clinical notes |

The full medallion runs as a **scheduled Databricks serverless job** (bronze → silver → gold →
drift monitor), with a local **Kafka** wearable stream exercised as a real streaming source.

See [Architecture](architecture.md) for the full medallion design and the healthcare layer
(FHIR → OMOP, de-identification at the silver boundary, coded-vocabulary data quality).

## Stack

Databricks (Delta + Unity Catalog) · Python + SQL · dbt · Airflow · PySpark + Structured Streaming ·
Kafka · Feast · pgvector · MLflow · Great Expectations · Terraform.

[View the code on GitHub →](https://github.com/joaoblasques/vitals){ .md-button .md-button--primary }
