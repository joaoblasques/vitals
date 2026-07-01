# ADR 0009 — Great Expectations as the gated silver DQ contract

**Status:** accepted · 2026-07-01

## Context
The project promises "coded-vocabulary data quality — validate vocabularies as DQ contracts (Great
Expectations), not vibes" and "DQ gates before exposing data." But silver DQ was a *descriptive* report
(`dq_report.json` — metrics, only the PHI check actually failed the build), and dbt tests gated only
**gold**. Great Expectations was named in the stack but unused.

## Decision
Make GE the **gating** DQ contract for **silver** (the consumption layer / PHI boundary). A code-defined
suite (`src/vitals/dq.py`) validates the conformed silver tables with GX Core 1.x and **exits non-zero on
any violation**; `make dq` runs it and **CI runs it after `make build`**, so it can't be skipped.

Signature expectations (the health-data DQ that sets this apart):
- **Coded-vocabulary value-sets:** every `condition.icd10_code` is not-null and ∈ the valid ICD-10 set
  (from `vitals.vocab`); `observation.metric` ∈ the standard set; glucose `unit_std` == `mg/dL`
  (unit standardization held). *Validated, not vibed.*
- **PHI boundary:** `silver.patient` columns match the allowed set exactly (no identifier can sneak in).
- **Ranges + key:** PRO score 0–100, steps 0–50000, `patient_key` unique + not-null.

Key choices:
- **A gate runs, it doesn't skip.** Unlike the Feast/pgvector *demos* (optional extras that skip in CI),
  GE is a gate — CI installs the `dq` extra and runs it. Clone-and-run stays lean (`make run` needs no GE).
- **Value-sets from version-controlled `vocab`** (DRY) — the same standards silver conforms to.
- **Complements, not replaces** the descriptive `dq_report.json`. dbt still gates gold; GE gates silver.

## Consequences
- New `dq` dependency group (`great-expectations>=1.0`); a `make dq` gate; `data/ge_validation.json`.
- CI has a new hard gate on silver — a violated expectation fails the build.

## Alternatives considered
- **Keep the descriptive report:** metrics you read after the fact don't *stop* bad data reaching gold.
- **dbt tests only:** they gate gold, not the silver conform + the coded-vocabulary contract at the PHI
  boundary; GE's value-set expectations express "valid vocabulary" first-class.
- **GE Data Docs site / GE-on-Databricks:** out of scope (a lean gate + JSON result; local DuckDB silver).
