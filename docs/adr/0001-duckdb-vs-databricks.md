# ADR 0001 — DuckDB for the MVP, Databricks as the target

**Status:** accepted · 2026-06-23

## Context
The project must (a) demonstrate a Databricks/Delta lakehouse and (b) be reviewable — ideally
clone-and-run — by someone evaluating it for a role.

## Decision
Build the runnable MVP on **DuckDB** (a single-file, zero-infra engine) and treat **Databricks +
Delta + Unity Catalog** as the documented deployment target. **PySpark** is the scale path (Phase 3
ships real Spark Structured Streaming + a window transform).

## Consequences
- `make run` reproduces the entire pipeline in seconds with no cloud account or cluster.
- The dbt project (`dbt-duckdb`) is one profile change away from `dbt-databricks`; the medallion
  SQL is portable.
- Trade-off: DuckDB is single-node. Mitigated by the PySpark modules that show the at-scale path.
- This is the JD's "ship the 80%, then scale" pragmatism made concrete.
