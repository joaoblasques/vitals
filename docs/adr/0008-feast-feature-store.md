# ADR 0008 — Feast feature store (the third gold store)

**Status:** accepted · 2026-06-30

## Context
The gold layer serves three shapes: analytics marts (dbt), a vector index (pgvector), and an ML
**feature store**. The feature store was scaffolded (`ml/feature_store/` — an entity + an 8-field
FeatureView + a local sqlite/file config) but never applied, materialized, or retrieved, so the
"three-store gold" claim had a hole.

## Decision
Make it real with **Feast**, local: sqlite online store + file offline store, sourced from the gold
feature parquet the pipeline already produces. Demonstrate the two things a feature store exists for:
- **Online retrieval** (`get_online_features`) — low-latency features for inference.
- **Point-in-time historical retrieval** (`get_historical_features`) — a leakage-safe training join
  over an entity dataframe (the same no-leakage discipline as the `surgery_90d` label).

Both are **parity-checked** against the offline parquet — the store must return the values the pipeline
produced (NULL-aware, float-tolerant).

Key choices:
- **Optional `feast` extra + graceful skip.** `serve.py` runs the demo only when Feast is installed
  (like the pgvector store); `make build` (`--no-serve`) never touches it, so clone-and-run and the
  hermetic CI gate are unchanged. The integration test skips in CI.
- **Deterministic, TTL-safe timestamps.** A fixed `event_timestamp` (2026-01-01), materialize end, and
  query time all sit inside the FeatureView's 90-day ttl window; tz-aware UTC throughout. Otherwise
  retrieval silently returns null.
- **The model keeps training on the parquet.** The historical demo proves PIT retrieval is correct;
  rewiring the MLflow model through Feast is out of scope.

## Consequences
- New runnable path: `make feast-demo` (+ the demo runs inside `make run` when the extra is present);
  `results.json` gains `feature_store.online_parity` / `historical_parity`.
- Registry + online sqlite are gitignored build artifacts.
- Production would point the offline store at Databricks/Delta — noted, not exercised (local is the
  deliverable, same stance as MetricFlow in ADR 0007).

## Alternatives considered
- **Leave it as a parquet + a table:** no online serving, no point-in-time joins — a feature *file*,
  not a feature *store*; the three-store claim stays half-true.
- **Online retrieval only:** simpler, but omits the point-in-time training join, which is the deeper
  ML-correctness (no-leakage) story.
- **Feast on Databricks/Delta now:** out of scope for a local, reproducible showcase.
