# Design — Feast feature store (make the third gold store real)

_Date: 2026-06-30 · Status: DRAFT — approved design, not yet implemented · Phase: gold three-store layer (closes the feature-store leg)_

> **One-liner:** turn the scaffolded Feast definitions into a working feature store — `apply` →
> `materialize` (offline parquet → online sqlite) → demonstrate **online** retrieval (inference) and
> **point-in-time historical** retrieval (leakage-safe training join), each **parity-proven** against
> the offline parquet. Wired into `serve.py` with the pgvector-style optional-extra skip, so
> clone-and-run and the hermetic CI gate stay intact.

## Goal

The README advertises a **three-store gold layer** — analytics marts (done), a **feature store**
(Feast), and a vector index (done). The feature store is the only leg that's *scaffolded but not
real*: `ml/feature_store/features.py` defines a `patient` entity + an 8-field `FeatureView`, and
`feature_store.yaml` configures a local sqlite/file store, but nothing ever runs `feast apply`,
materializes the online store, or retrieves a feature. This unit makes it real and demonstrates the
two things a feature store exists for — **online serving** and **point-in-time-correct offline
retrieval** — so the "three stores" claim is true and defensible.

## Non-negotiable principles this serves / preserves

- **Reproducible + idempotent.** The offline `event_timestamp` is a fixed deterministic value; `apply`
  / `materialize` are re-runnable (overwrite the registry + online store).
- **Clone-and-run / hermetic CI unaffected.** Feast is an **optional `feast` extra**; `make build`
  (`--no-serve`) never touches it; the demo + parity test skip when Feast isn't installed (exactly the
  pgvector / MetricFlow pattern).
- **Verify against known truth.** Online + historical retrieval are **parity-checked** against the
  offline parquet — the feature store must return the same values the pipeline produced.
- **AI-ready data, proven not asserted.** The point-in-time historical join is the no-label-leakage
  story the project already tells (the surgery_90d label); demonstrating it makes that concrete.

## Scope decisions (locked with the user)

- **Demonstrate online AND point-in-time historical retrieval** (not online-only).
- **The MLflow demo model keeps training on the parquet** — do NOT repoint `_train_model` through
  `get_historical_features`. The historical demo *proves* PIT retrieval is correct (parity); rewiring
  the working model into Feast is out of scope.
- **Local only** — sqlite online + file offline. No Feast on Databricks/Delta (the `feature_store.yaml`
  comment notes production would point there; exercising it is out of scope, like MetricFlow stayed
  local).

## Current state

- `ml/feature_store/feature_store.yaml` — project `vitals`, provider `local`, online `sqlite`
  (`online_store.db`), offline `file`, registry `registry.db`.
- `ml/feature_store/features.py` — `patient` entity (join key `patient_key`);
  `patient_surgery_risk_features` FeatureView, ttl 90d, `online=True`, 8 fields (`age`, `mean_pain`,
  `last_pain`, `pain_trend`, `mean_adherence`, `mean_glucose_mgdl`, `mean_hr`, `n_observations`);
  `FileSource(path=data/gold/patient_features.parquet, timestamp_field="event_timestamp")`.
- `serve.py` `run()` builds `feats` (`FEATURE_SQL`) and writes `data/gold/patient_features.parquet`,
  but the parquet has **no `event_timestamp`** column (Feast's `FileSource` requires it) and Feast is
  never invoked. `_rag_demo` is the lazy-try-with-fallback pattern to mirror.

## Components

### 1. `serve.py` — add a deterministic `event_timestamp` to the feature parquet

Before writing the parquet, stamp every row with a fixed event timestamp (so materialize + PIT are
reproducible, same discipline as `metric_date`):

```python
feats["event_timestamp"] = pd.Timestamp("2026-01-01")
feats.to_parquet(GOLD / "patient_features.parquet", index=False)
```

(The `gold.patient_features` DuckDB table may keep or drop the column — the parquet is Feast's source.)
The 8 FeatureView fields must all exist in `feats` (verify names/types; `age` + the 7 observation/claim
features). No FeatureView change unless a name/type mismatch is found.

### 2. `src/vitals/feature_store.py` (new) — the driver (mirrors `vector_index.py`)

Pure helpers (hermetic-testable) separated from lazy Feast I/O:

- **Pure:** `entity_df(keys: list[str], at: str) -> pd.DataFrame` (build the entity dataframe:
  `patient_key` + `event_timestamp` for a PIT query); `parity(retrieved: dict|DataFrame, offline:
  pd.DataFrame, keys, features) -> dict` (compare retrieved feature values to the offline parquet rows,
  to a float tolerance; return `{feature: ok}` + an `all_match` bool); `FEATURES` (the 8 field names),
  `REPO = ml/feature_store`, `PARQUET = data/gold/patient_features.parquet`, `EVENT_TS = "2026-01-01"`.
- **Lazy I/O** (import `feast` inside the functions): `is_available()` (feast importable + parquet
  exists); `store()` (`FeatureStore(repo_path=REPO)`); `apply_materialize()` (run **`feast apply` as a
  subprocess in `REPO`** — the documented repo-scan path that reads `features.py` — then
  `store.materialize_incremental(end_date=MATERIALIZE_END)`); `online_features(store, keys)`
  (`store.get_online_features(features=[...], entity_rows=[{"patient_key": k}...]).to_dict()`);
  `historical_features(store, keys)` (`store.get_historical_features(entity_df(keys, QUERY_TS),
  features=[...]).to_df()`); `demo()` (apply+materialize → online for sample keys → historical for the
  same keys → parity both vs offline → return a results dict); `main()` (CLI: `apply` | `materialize` |
  `online <key>` | `historical` | `demo`).

**Timestamp / TTL constraint (must get right, or retrieval silently returns null):** the FeatureView
ttl is **90 days**. The offline `EVENT_TS` (2026-01-01), the `MATERIALIZE_END`, and the historical
`QUERY_TS` must all fall within one 90-day window of each other, or the online store serves nothing and
PIT returns null. Pin deterministic constants: `EVENT_TS = "2026-01-01"`, `MATERIALIZE_END =
"2026-03-01"`, `QUERY_TS = "2026-03-01"` (≤ 90d after EVENT_TS, both fixed → reproducible). The parity
check (retrieved == offline, non-null) is what catches a TTL/timestamp mistake.

### 3. `serve.py` — wire `_feature_store_demo` (pgvector-style)

```python
def _feature_store_demo(feats):
    try:
        from vitals import feature_store as fs
        if not fs.is_available():
            return {"store": "feast (skipped: not installed)"}
        return fs.demo()            # {online_parity, historical_parity, n_served, ...}
    except Exception as e:
        return {"store": f"feast (skipped: {e})"}
```

Call it in `run()`'s feature-store section; merge its result into `results["feature_store"]` (alongside
the existing `n_patients`/`features`/parquet). When skipped, the offline parquet section stays as today.

### 4. `Makefile`

```make
feast-demo:   ## apply + materialize Feast (offline parquet -> sqlite online) + online/historical retrieval
	PYTHONPATH=src ./.venv/bin/python -m vitals.feature_store demo
```

(Needs `uv sync --extra feast` + a prior `make run` to have written the parquet.)

### 5. `pyproject.toml` / `.gitignore`

- `feast` extra already exists (`feast>=0.40`) — keep. Confirm it resolves with the lean-core deps.
- Gitignore Feast build artifacts: `ml/feature_store/data/`, `ml/feature_store/registry.db`,
  `ml/feature_store/online_store.db` (and any `*.db` the local store writes).

## Data flow

```
serve.FEATURE_SQL ──► feats (+ event_timestamp) ──► data/gold/patient_features.parquet  (OFFLINE source)
        feast apply (registry.db) ──► materialize ──► online_store.db (sqlite, latest per patient)
            ├─ get_online_features([patient_key]) ─────────► parity == offline row   (inference)
            └─ get_historical_features(entity_df @ t) ─────► parity == offline values (PIT training join)
```

## Error handling / gates

- **Missing `event_timestamp`** → Feast `apply`/`materialize` errors; fixed by component 1.
- **Feast not installed / parquet absent** → `is_available()` false → `_feature_store_demo` returns a
  "skipped" note; serve + clone-and-run continue (offline parquet unaffected).
- **Parity mismatch** (online/historical ≠ offline) → the parity test fails; surfaces a real bug rather
  than passing silently.

## Testing

- **Hermetic CI (no feast):** unchanged — `make build` (`--no-serve`) never invokes Feast; the suite's
  Feast tests skip via `importorskip("feast")`.
- **Pure-helper unit tests** (`tests/test_feature_store.py`, hermetic): `entity_df` shape/columns;
  `parity` returns all-match on equal values and flags a deliberately perturbed value; `FEATURES`
  matches the FeatureView.
- **Gated integration/parity test** (same file, `importorskip("feast")` + skip if the parquet is
  absent): build the store from the committed/`make run` parquet, assert `get_online_features` for
  sample `patient_key`s equals the offline parquet rows (to tolerance), and `get_historical_features`
  for an entity df equals the offline values. Skips in CI.
- **Local end-to-end:** `make run` (writes the parquet + runs `_feature_store_demo` when feast present)
  then `make feast-demo`.

## Docs

- New ADR `docs/adr/0008-feast-feature-store.md` — Feast local (sqlite online + file offline) as the
  feature-store leg of the three-store gold; online vs point-in-time-historical retrieval (and why a
  feature store beats a parquet: online serving + leakage-safe training joins); offline source = the
  gold feature parquet; production-on-Databricks noted but not exercised; parity vs the offline parquet
  as the correctness contract; optional-extra / CI-skip clone-and-run discipline.
- `README.md` — the "Feature store | Feast (offline + online)" row becomes true; add `make feast-demo`.
- `results.json` gains `feature_store.online_parity` / `historical_parity` when feast is present.

## Non-goals (YAGNI)

- No online-serving HTTP server (in-process `get_online_features` only).
- No on-demand / transformation feature views; no expanding the FeatureView beyond its 8 fields.
- No repointing the MLflow model to train via `get_historical_features` (model stays on the parquet).
- No Feast on Databricks/Delta (local file+sqlite is the deliverable).

## Files touched

| File | Change |
|---|---|
| `src/vitals/serve.py` | add deterministic `event_timestamp` to the parquet; wire `_feature_store_demo` into `run()` |
| `src/vitals/feature_store.py` | new — pure helpers + lazy Feast driver + CLI |
| `ml/feature_store/features.py` | verify field names/types vs the parquet (change only on mismatch) |
| `Makefile` | `feast-demo` target |
| `.gitignore` | Feast build artifacts (`registry.db`, `online_store.db`, `ml/feature_store/data/`) |
| `tests/test_feature_store.py` | new — hermetic pure-helper tests + gated parity test |
| `docs/adr/0008-feast-feature-store.md` | new ADR |
| `README.md` | feature-store row true + `make feast-demo` note |
