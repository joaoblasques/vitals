# Design — full-medallion `python_wheel_task` (bronze + silver in the bundle job)

_Date: 2026-06-30 · Status: DRAFT — approved design, not yet implemented · Phase: Databricks production path (closes ADR 0005's open follow-up)_

> **One-liner:** promote bronze + silver into the deployed Asset Bundle job as a single
> `python_wheel_task` (generate → bronze Delta → silver Delta, with the PHI gate), wired **before**
> the existing `gold_dbt → drift_monitor`, so one scheduled serverless run does the **whole medallion**
> on Databricks, unattended. The databricks-connect dev path stays untouched.

## Goal

Today the deployed job (`databricks.yml`) only runs **gold** (dbt marts + tests) and **drift**. Bronze
and silver are populated by hand from the laptop via databricks-connect (`make bronze-/silver-databricks`).
ADR 0005's standing follow-up is to make the job **self-contained**: a scheduled run should generate the
synthetic data and build bronze + silver on-cluster, then feed the existing gold + drift tasks — the
honest "this is how it ships in a real shop" artifact. This unit does exactly that.

## Non-negotiable principles this serves / preserves

- **Reproducible from code.** The whole medallion deploys + runs from a terminal (`bundle deploy`/`run`),
  no manual GUI, no laptop in the loop.
- **PHI boundary at silver.** The in-job gate hard-fails if any HIPAA identifier reaches `silver.patient`
  (`assert_no_phi`) — the boundary is enforced on-cluster, not just locally.
- **Idempotent pipelines.** Generation is deterministic (seeded RNG); bronze landing + silver build are
  overwrite — re-runs/backfills produce identical data and never duplicate.
- **Build with connect, ship with the bundle (ADR 0005).** This adds the production path; the connect dev
  loop (`make bronze-/silver-databricks`) keeps working unchanged — one shared codebase, two entry points.
- **Clone-and-run / hermetic CI unaffected.** All defaults are unchanged; the new behavior only activates
  under env signals the bundle sets. New tests are hermetic (no Databricks).

## Scope decision (locked with the user)

**Single `medallion_ingest` task** for generate + bronze + silver (not three tasks). Reason: those three
stages are one execution substrate — plain Python + a shared PySpark session — and hand data off
in-process (generate → `/tmp` NDJSON → bronze Delta → silver Delta). Splitting them would break the
in-process/`/tmp` handoff, force a volume round-trip, and pay three serverless cold starts to manufacture
a boundary the data flow doesn't want. Task boundaries are drawn at **runtime/compute changes**, which is
why `gold_dbt` (the dbt tool on a SQL warehouse) and `drift_monitor` (numpy/pandas, and must run *after*
gold) already are — and stay — separate tasks. **Acceptance bar (locked):** a real `bundle deploy && bundle
run` reaching `TERMINATED SUCCESS`, same bar as the gold job.

## Architecture

```
            ┌─────────────────────────── one serverless job: vitals-gold-refresh ───────────────────────────┐
 schedule → │  medallion_ingest (python_wheel_task)         gold_dbt (dbt_task)        drift_monitor (spark_python_task) │
            │  generate → bronze Δ → silver Δ + PHI gate  →  dbt build + 26 tests   →   PSI drift → monitoring   │
            │  [wheel: vitals, ambient serverless Spark]    [SQL warehouse]             [py_env: numpy/pandas]    │
            └────────────────────────────────────────────────────────────────────────────────────────────────┘
```

`medallion_ingest` runs the wheel's `vitals-medallion` entry point on serverless compute; `gold_dbt`
gains `depends_on: medallion_ingest`; `drift_monitor` keeps `depends_on: gold_dbt`.

## Components

### 1. `src/vitals/env.py` (new) — call-time resolution of the two env signals

A tiny dependency-free helper so both `generate` and the Databricks backend resolve the writable bronze
dir + the Spark mode **at call time** (not import time → testable, no import-order traps):

```python
import os
from pathlib import Path

_DEFAULT_BRONZE = Path(__file__).resolve().parents[2] / "data" / "bronze"

def bronze_dir() -> Path:
    """Where raw NDJSON is written/read. Default = repo data/bronze (local + connect unchanged);
    the bundle's medallion task overrides it to a writable /tmp dir via VITALS_BRONZE_DIR."""
    return Path(os.environ.get("VITALS_BRONZE_DIR", str(_DEFAULT_BRONZE)))

def spark_mode() -> str:
    """'ambient' when running ON Databricks compute (the wheel entry point sets it); 'serverless'
    for the databricks-connect dev path (laptop drives remote serverless). Default 'serverless'
    keeps the connect path unchanged."""
    return os.environ.get("VITALS_SPARK_MODE", "serverless")
```

### 2. `src/vitals/generate.py` — write to `bronze_dir()` instead of the hardcoded constant

`generate()` currently writes into the module constant `BRONZE = <repo>/data/bronze`. Resolve the
output dir from `env.bronze_dir()` at call time (default identical → local/CI/connect untouched). The
per-resource writer takes the resolved dir. No new third-party deps (generate stays pure-stdlib).

### 3. `src/vitals/backends/databricks_delta.py` — honor `bronze_dir()` + branch `_spark()`

- `_upload_landing()` / `local_counts()` read NDJSON from `env.bronze_dir()` (was the module
  `BRONZE_DIR` constant) — so on-cluster they see the freshly generated `/tmp` files.
- `_spark()` branches on `env.spark_mode()`:

```python
def _spark():
    from databricks.connect import DatabricksSession
    b = DatabricksSession.builder
    if env.spark_mode() == "ambient":
        return b.getOrCreate()            # ON Databricks: the ambient serverless session
    return b.serverless().getOrCreate()   # connect from laptop (unchanged)
```

### 4. `src/vitals/medallion_job.py` (new) — the wheel entry point

```python
"""On-cluster full-medallion entry point (the bundle's python_wheel_task: `vitals-medallion`).
Generate synthetic data, land bronze Delta, build silver Delta, enforce the PHI + non-empty gates.
Runs ONLY on Databricks serverless — sets VITALS_SPARK_MODE=ambient so the shared backend uses the
ambient session. The bronze dir (a writable /tmp path) arrives as the wheel parameter argv[0]."""
import os
import sys

def _assert_nonempty(bronze: dict[str, int], silver: dict[str, int]) -> None:
    empties = [k for d in (bronze, silver) for k, v in d.items() if v <= 0]
    if empties:
        raise AssertionError(f"empty tables after ingest: {sorted(empties)}")

def main(argv: list[str] | None = None) -> None:
    argv = sys.argv[1:] if argv is None else argv
    if argv:
        os.environ["VITALS_BRONZE_DIR"] = argv[0]
    os.environ["VITALS_SPARK_MODE"] = "ambient"
    from vitals import generate
    from vitals.backends import databricks_delta as dx
    print(f"[medallion] generate -> {os.environ.get('VITALS_BRONZE_DIR')}")
    generate.generate()
    bronze = dx.land_bronze()                            # upload to volume + Delta
    silver = dx.build_silver()                           # bronze Delta -> de-identified silver
    dx.assert_no_phi(dx.silver_patient_columns())        # PHI boundary — hard gate
    _assert_nonempty(bronze, silver)                     # non-empty — hard gate
    print(f"✅ medallion ingest complete: bronze={sum(bronze.values())} silver={sum(silver.values())}")
```

The gates `raise` → the task fails → the job fails → the existing `on_failure` alert fires.

### 5. `pyproject.toml` — make it wheel-buildable + declare the entry point

- Add `[build-system]` (setuptools backend) — required for `uv build --wheel`.
- Add `[project.scripts]`: `vitals-medallion = "vitals.medallion_job:main"`.
- Ensure `[project] version` exists (needed for a wheel filename).

### 6. `databricks.yml` — wheel artifact + the `medallion_ingest` task

- `artifacts:` block builds the wheel (`uv build --wheel`).
- A serverless `ingest_env` environment whose dependencies include the built wheel (DAB resolves the
  artifact; **exact dependency glob/syntax pinned in the plan against current DAB docs** — this is the
  Free-Edition-serverless detail ADR 0005 flagged).
- The `medallion_ingest` `python_wheel_task` (`package_name: vitals`, `entry_point: vitals-medallion`,
  `parameters: ["/tmp/vitals_bronze"]`).
- `gold_dbt` gains `depends_on: medallion_ingest`.

## Data flow (on-cluster, one scheduled run)

```
generate.generate()  ──writes NDJSON──►  /tmp/vitals_bronze/*.ndjson
   └─ land_bronze(): upload /tmp → /Volumes/vitals_bronze/raw/landing → Delta vitals_bronze.raw.*
        └─ build_silver(): vitals_bronze.raw.* → de-identified Delta vitals_silver.clinical.*
             └─ assert_no_phi + non-empty gates ──► gold_dbt (marts + 26 tests) ──► drift_monitor (PSI)
```

## Error handling / gates

- **PHI leak** → `assert_no_phi` raises → task fails (the signature gate, now enforced on-cluster).
- **Empty table** (generation/landing silently produced nothing) → `_assert_nonempty` raises → task fails.
- **dbt test failure** (gold) → `gold_dbt` fails (existing).
- Any failure pages the deploying user via the existing `email_notifications.on_failure`.
- **Not an in-job gate:** cross-engine row-count parity vs the local DuckDB baseline — those baselines
  live in `data/` (outside the wheel) and the local warehouse doesn't exist on-cluster. Parity remains a
  **dev/connect-time** gate (`make bronze-/silver-databricks`), where the DuckDB baseline exists.

## Testing

- **Hermetic unit tests** (`tests/test_medallion_job.py`, CI-safe, no Databricks):
  - `env.bronze_dir()` honors `VITALS_BRONZE_DIR`, else the repo default; `env.spark_mode()` defaults to
    `serverless`, returns `ambient` when set.
  - `_assert_nonempty` passes on all-positive counts, raises on a zero/negative count.
  - `medallion_job.main` (with `generate`/`databricks_delta` mocked) sets both env vars and calls
    generate → land_bronze → build_silver → assert_no_phi → _assert_nonempty **in order**.
  - `generate.generate(... )` writes into a `tmp_path` when `VITALS_BRONZE_DIR` points there (proves the
    dir override end-to-end without Databricks).
- **Clone-and-run + CI:** unchanged DuckDB path; `make build` + the suite stay green (defaults unchanged).
- **Live acceptance (the bar):** `source infra/terraform/.env && databricks bundle deploy && databricks
  bundle run vitals_medallion` → `TERMINATED SUCCESS`; all three tasks green; spot-check
  `vitals_silver.clinical.patient` has no PHI columns and `vitals_gold.marts.*` rebuilt.

## Docs

- **ADR 0005** — third `Update (2026-06-30)`: the open follow-up is **closed**; the job now runs the full
  medallion (single `python_wheel_task` for generate+bronze+silver; the runtime-boundary rule that keeps
  gold/drift separate; explicit `VITALS_SPARK_MODE` over runtime sniffing; PHI+non-empty in-job gates).
- **databricks.yml** comments — drop the "promoting them is a documented follow-up" note (now done).
- **README / Makefile** — one line noting `bundle run` now runs the whole medallion.

## Non-goals (YAGNI)

- No streaming/Kafka in the job (the batch medallion is the deliverable).
- No non-serverless cluster (Free Edition is serverless-only).
- No change to the databricks-connect dev path's behavior, or to the DuckDB clone-and-run default.
- No splitting bronze/silver into separate tasks (single-substrate, in-process handoff).
- No removal of the existing parity tooling (it stays the connect-time gate).

## Files touched

| File | Change |
|---|---|
| `src/vitals/env.py` | new — `bronze_dir()` + `spark_mode()` call-time resolvers |
| `src/vitals/generate.py` | write to `env.bronze_dir()` (default unchanged) |
| `src/vitals/backends/databricks_delta.py` | read `env.bronze_dir()`; branch `_spark()` on `env.spark_mode()` |
| `src/vitals/medallion_job.py` | new — `vitals-medallion` entry point + gates |
| `pyproject.toml` | `[build-system]`, `[project.scripts]`, ensure `version` |
| `databricks.yml` | wheel `artifacts` + `medallion_ingest` task + `ingest_env`; `gold_dbt` depends_on |
| `tests/test_medallion_job.py` | new — hermetic unit tests |
| `docs/adr/0005-spark-execution-databricks-connect.md` | third Update (follow-up closed) |
| `README.md` / `Makefile` | one-line note |
