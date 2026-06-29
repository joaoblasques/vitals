# ADR 0007 — dbt Semantic Layer (MetricFlow) over a per-patient base

**Status:** accepted · 2026-06-29

## Context
The README promises "analytics marts — dbt (Kimball star) + semantic layer", but the marts
(`mart_condition_outcomes`/`mart_cost_outcomes`) hard-coded their aggregations in SQL and duplicated
the same per-patient rollups. There was no declarative, composable metric layer — the thing that lets
analytics, BI, and ad-hoc queries share one definition of "surgery rate" or "avg conservative spend".

## Decision
Add a real dbt **Semantic Layer** (MetricFlow): `semantic_models` + `metrics` defined over a new
per-patient base model, `fct_patient_metrics`. The two marts are refactored to `group by` that same
base (outputs unchanged), so the per-patient aggregation lives in exactly one place and the marts and
the semantic layer can't disagree.

Key choices:
- **Per-patient base, not observation-grain.** The marts average per-patient first (mean of patient
  means); semantic measures must aggregate over a one-row-per-patient table to reproduce that. Hence
  `fct_patient_metrics`.
- **MetricFlow is an optional `metrics` extra.** `dbt build` parses the semantic YAML natively, so the
  hermetic, no-extra CI gate validates the config for free. `mf` (validate/query) and the parity test
  are local-only — clone-and-run and CI never depend on MetricFlow.
- **Parity is the correctness contract.** A gated test proves `mf query` equals the mart numbers per
  condition; the marts are the trusted reference.
- **Time spine required.** dbt 1.11's MetricFlow validation requires a time spine model and an
  `agg_time_dimension` on every measure. A `metricflow_time_spine.sql` model and a fixed `metric_date`
  dimension (`date '2026-01-01'`) are added to `fct_patient_metrics` to satisfy this constraint;
  they do not affect parity (all queries group by `patient__primary_condition`, not by date).

## Consequences
- New `metrics` dependency group (`dbt-metricflow[duckdb]`); not core. `make metrics-validate` /
  `metrics-list` / `metrics-query` drive it.
- The semantic YAML is adapter-agnostic — it would run against the Databricks target too — but
  exercising MetricFlow on Databricks is out of scope (local DuckDB is the deliverable).
- `fct_patient_metrics` is a new gold table; the marts now depend on it (one extra build node).

## Alternatives considered
- **Observation-grain measures (no base table):** simpler YAML, but the metrics would not match the
  marts' patient-first averaging — no parity, two conflicting definitions of the same metric.
- **Keep hand-rolled marts only:** no composability; "semantic layer" stays a label, not a capability.
- **dbt Cloud / hosted Semantic Layer API:** out of scope for a local, reproducible showcase.
