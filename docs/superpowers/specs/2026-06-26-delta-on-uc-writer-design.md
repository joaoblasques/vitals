# Design — Wire the pipeline to Delta-on-UC (writer abstraction)

_Date: 2026-06-26 · Status: draft (for review) · Phase: Phase 5 — the deployment follow-up to Phase 0 UC IaC_

## Goal

Let the existing Vitals pipeline write its medallion layers to **Delta tables in Unity Catalog**
(the catalogs Terraform now provisions: `vitals_bronze/silver/gold`) **without losing** the
clone-and-run local DuckDB path. The target is a **switch**, not a rewrite: `VITALS_TARGET=local`
(default) keeps today's behavior; `VITALS_TARGET=databricks` writes Delta-on-UC.

This is the explicit follow-up the UC IaC spec deferred ("Non-goal: wiring the existing local
DuckDB pipeline to write Delta-on-UC. That is the natural follow-up unit.").

## Current state (what we're abstracting)

| Stage | Code | Engine | Output today |
|---|---|---|---|
| generate bronze | `generate.py` | Python | NDJSON → `data/bronze/*.ndjson` |
| bronze→silver | `lakehouse.py` | **DuckDB SQL** | DuckDB tables `bronze.*`, `silver.*` in `data/vitals.duckdb` |
| (scale path) | `spark_silver.py` | **PySpark** | already a parallel impl of silver |
| silver→gold | `dbt` | dbt-duckdb | `gold.*` in the same DuckDB file (profile `dev`) |
| serve | `serve.py` | Python | features (Feast/Parquet) + vectors + MLflow model |

Two facts drive the design:
1. **The silver SQL is DuckDB-dialect** (`read_json(... union_by_name)`, struct access like
   `code.coding[1].code`, `md5()`, `strftime`). It does **not** run unchanged on Spark — which is
   exactly why `spark_silver.py` already exists as a separate implementation. So the seam is **not**
   "same SQL, swap the connection"; it's **per-backend transforms that satisfy the same contracts**.
2. **dbt already has a clean target seam** — silver→gold is just a dbt profile. Adding a
   `databricks` output to `dbt/profiles.yml` is the whole gold story.

## Decision: a thin backend interface, two implementations

Introduce `src/vitals/backends/` with a small protocol the run-loop calls. Each backend owns its
engine-specific transform but they emit the **same logical tables** and run the **same DQ
assertions**.

```python
class LakehouseBackend(Protocol):
    def land_bronze(self) -> None:          # raw NDJSON → bronze tables
    def build_silver(self) -> dict:         # bronze → de-identified silver; returns DQ report
    def gold_target(self) -> str:           # dbt target name to use ("dev" | "databricks")
    def verify(self) -> dict:               # row counts + PHI-absence assertion
```

- `LocalDuckDBBackend` — wraps **today's `lakehouse.py`** verbatim (no behavior change). `gold_target() == "dev"`.
- `DatabricksDeltaBackend` — lands NDJSON into the UC **volume** `vitals_bronze.raw.landing`, writes
  Delta via Spark, runs `spark_silver.py`'s logic, `gold_target() == "databricks"`.

`run.py` picks the backend from `VITALS_TARGET` and is otherwise unchanged. Backend selection is the
**only** new branch in the orchestration.

## Layer-by-layer mapping (local → Databricks)

| Layer | Local (DuckDB) | Databricks (Delta-on-UC) |
|---|---|---|
| Raw landing | `data/bronze/*.ndjson` | upload NDJSON to volume `/Volumes/vitals_bronze/raw/landing/` |
| Bronze tables | `bronze.*` DuckDB | Delta tables in `vitals_bronze.raw.*` (read NDJSON from the volume) |
| Silver | `silver.*` DuckDB | Delta tables in `vitals_silver.clinical.*` (+ `omop` for OMOP) |
| Gold | dbt `dev` → DuckDB `gold` | dbt `databricks` → `vitals_gold.{marts,features,vectors,monitoring}` |
| PHI boundary | assertion on `silver.patient` cols | **same assertion** + enforced by UC grant (analysts: silver/gold only) |

## Auth & compute (Free Edition reality)

- Free Edition is **serverless-only**. The Databricks backend connects with **databricks-connect**
  (Spark Connect) to serverless, authenticated by the same `DATABRICKS_HOST` / `DATABRICKS_TOKEN`
  used by Terraform. No cluster to manage.
- dbt uses **dbt-databricks** (already an optional dep) against the same host/token + an HTTP path.
- All creds via env vars / `.env` (gitignored). **Never committed** — same rule as the IaC.
- New optional dep group already partly present: `databricks = ["pyspark", "dbt-databricks"]`; add
  `databricks-connect` and the `databricks-sdk` (for the volume upload).

## DQ parity — the contract is the spec

Both backends must produce identical DQ verdicts. The de-id assertion (`silver.patient` carries no
`name/identifier/address/birthDate`) and the row-count/coding-percentage checks in
`lakehouse._dq_silver` become **backend-agnostic checks** run against whichever engine. A
**row-count parity test** (local vs UC for the same seed) is the acceptance gate — this honors the
project's "verify every step against row counts" principle even across engines.

## Idempotency

Delta writes use **overwrite by run** (`CREATE OR REPLACE TABLE` / `saveAsTable(mode="overwrite")`)
keyed to the deterministic seed, matching the local `CREATE OR REPLACE`. Re-runs/backfills must not
duplicate — same guarantee both backends. (Natural-key MERGE is a later refinement; overwrite is
correct for the demo's full-refresh model.)

## Incremental rollout (do NOT one-shot)

1. **Bronze only.** Backend interface + `DatabricksDeltaBackend.land_bronze()`. Verify the volume
   upload + Delta `vitals_bronze.raw.*` row counts == local bronze. Stop, confirm.
2. **Silver.** Port `spark_silver.py` to write `vitals_silver.clinical.*`; run the de-id assertion
   on UC; row-count parity vs local silver. Stop, confirm.
3. **Gold.** Add `databricks` output to `dbt/profiles.yml`; `dbt build --target databricks`;
   verify gold marts/features. Stop, confirm.
4. **Serve + CI.** Optional: point Feast/vector/MLflow reads at gold-on-UC; add a CI job that runs
   `validate`-level checks (can't run real Delta in CI without a workspace — gate on local only).

Each step is independently verifiable and leaves a working system; the local path never breaks.

## Verification plan

- `VITALS_TARGET=local python -m vitals.run` → unchanged results (regression guard).
- `VITALS_TARGET=databricks python -m vitals.run` → Catalog Explorer shows populated
  `vitals_bronze/silver/gold`; `verify()` row counts match local within the seed; PHI assertion green.
- A pytest that asserts **local vs Databricks row-count parity** per table (skipped when no creds).

## Out of scope

- Streaming-to-Delta (`streaming.py`) and the OMOP `omop` schema population on UC — follow-ups.
- Real pgvector and Kafka — separate prod-target swaps, tracked elsewhere.
- Remote Terraform backend / multi-user grants enforcement — Premium concerns, already documented.

## Open questions (resolve at review)

- databricks-connect vs submitting a notebook/job for the Spark silver step — connect is simpler for
  a demo; confirm it works against Free Edition serverless from local.
- Keep `spark_silver.py` as the single silver source of truth and **delete the DuckDB silver SQL**,
  or keep both? Recommendation: **keep both** (DuckDB = clone-and-run demo; Spark = scale/prod),
  bound by the shared DQ contract. Divergence risk is mitigated by the parity test.
