# ADR 0004 — A three-store gold layer (marts + feature store + vector index)

**Status:** accepted · 2026-06-23

## Context
"AI-ready data" is not one thing. Analytics, classical ML, and LLM/RAG each need a different shape
of the same clean data.

## Decision
Serve gold three ways from one pipeline:
1. **Analytics marts** (dbt Kimball star + metric marts) — BI, cohorts, value-based-care reporting.
2. **Feature store** (Feast, offline + online) — time-windowed patient features for ML.
3. **Vector index** (pgvector in prod; TF-IDF in the MVP) — clinical-note retrieval for RAG.

## Consequences
- Maps directly onto a health-tech company's real surfaces (analytics; surgery-risk ML; LLM care
  assistants).
- The feature store can hold many features while a given model uses a curated subset (feature
  selection stays a modeling concern, not a pipeline one).
- Each store has a clear owner and contract, so changes are isolated.
