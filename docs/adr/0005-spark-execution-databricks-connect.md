# ADR 0005 — databricks-connect for the Delta-on-UC writer, job-submit as the production path

**Status:** accepted · 2026-06-26 · _both paths now implemented (see Update)_

## Context
The pipeline's local layers run on DuckDB (ADR 0001). Wiring them to write **Delta into Unity
Catalog** (the now-applied Free Edition workspace) requires running PySpark — and Free Edition is
**serverless-only**, so there is no local Spark and no cluster to attach to. The code lives on the
dev machine; the Spark execution must happen on Databricks. Two mechanisms bridge that gap:

1. **databricks-connect (Spark Connect):** local Python issues Spark commands over the wire to remote
   serverless compute; results stream back. The laptop stays the driver of control flow.
2. **Job / asset-bundle submit:** package the code, ship it to Databricks, and run it there as a job;
   the laptop only triggers and polls.

This decision is also a showcase artifact: the interviewer is judging *engineering judgment*, so the
choice and its reasoning matter as much as the result.

## Decision
Build the Delta-on-UC writer with **databricks-connect**, and document **job/asset-bundle submit as
the production deployment path** (implement it as a later follow-up, not now).

Rationale:
- **Fast feedback loop where we need it most.** We build incrementally and verify every layer
  against row counts/DQ expectations (project principle). Spark Connect gives interactive errors and
  the same `python -m vitals.run` entrypoint with only a target switch (`VITALS_TARGET=databricks`) —
  one codebase, no packaging round-trip per iteration.
- **Architecturally clean story.** It keeps the local-DuckDB and remote-Delta paths behind one
  backend abstraction, which is the design already drafted (see the writer design spec).
- **Production fluency is still demonstrated**, just sequenced second: an asset-bundle/job path is the
  honest "how this actually ships in a real shop" answer, and naming it shows we know the difference.

## Consequences
- Add `databricks-connect` (version-pinned to the workspace runtime — Spark Connect requires the
  client and server versions to line up) to the `databricks` optional-dependency group.
- The dev loop needs live creds (`infra/terraform/.env`) and a network connection; it is not
  clone-and-run. The **DuckDB path remains the clone-and-run default** (ADR 0001 unchanged).
- Follow-up (tracked, not done): a Databricks Asset Bundle (`databricks.yml`) + job definition so the
  same transforms run as a scheduled production job — the deployment half of the story.
- Interview narrative: "interactive dev with Spark Connect; production via asset bundles" — a
  deliberate, defensible split rather than a single tool used dogmatically.

## Alternatives considered
- **Job-submit only:** most production-like, but the upload→run→fetch-logs loop is too slow for the
  build-and-verify phase; would slow iteration without improving the final artifact.
- **A non-serverless cluster:** not available on Free Edition; moot.
- **Skip Spark, write Delta from local Python (e.g. delta-rs):** sidesteps Databricks compute
  entirely and wouldn't demonstrate the Spark-on-UC competency the role targets.

## Update (2026-06-26) — both paths implemented

**The frame: build with connect, ship with the bundle.** DEV = databricks-connect (laptop drives,
Databricks computes) for fast build-and-verify; PROD = an Asset Bundle serverless job (Databricks
owns it, scheduled, tests-as-gates, unattended). One shared codebase behind a target switch, so the
two paths can't drift — the cost is maintaining two entry points, justified because each optimizes a
different phase.

- **Dev (databricks-connect):** bronze + silver build Delta on UC interactively
  (`make bronze-/silver-databricks`), each gated by row-count + DQ parity vs local DuckDB.
- **Production (job-submit):** a Databricks Asset Bundle (`databricks.yml`) ships the gold stage as a
  scheduled **serverless job** (`make bundle-deploy` / `bundle-run`) — verified `TERMINATED SUCCESS`.
  Free-Edition specifics learned: bundle/`databricks api` need `DATABRICKS_AUTH_TYPE=pat` (the
  `.databrickscfg` DEFAULT profile is OAuth and otherwise breaks token refresh); the managed dbt task
  **auto-generates** its profile (target `databricks_cluster`, catalog/schema from the task fields),
  so the project's `--target databricks` is not used in-job.
- **Open follow-up:** promote bronze/silver into the job as a `python_wheel_task` (needs ambient
  serverless Spark via `DatabricksSession.builder.getOrCreate()`, a writable generate dir, and
  volume upload from the job) for a single full-medallion scheduled run.

## Update (2026-06-29) — the job is *operated*, not just shipped

Two ops additions so the deployed job behaves like something a real shop runs unattended:

- **Failure alerts.** Job-level `email_notifications.on_failure` pages a recipient when a run fails
  (a job that fails silently isn't operated). Recipient defaults to the deploying user via
  `${workspace.current_user.userName}` — **no address committed to this public repo** (same no-PII
  pattern as the Terraform UC grants); override with `BUNDLE_VAR_alert_email` / `--var`.
  `notification_settings.no_alert_for_skipped_runs` keeps the paused demo from spamming.
- **Drift monitoring as a job task.** A `drift_monitor` task (`spark_python_task`,
  `pipelines/drift_job.py`) runs **downstream of `gold_dbt`** on the same schedule, so PSI
  feature-drift is scored every time the data moves — not by a side process that rots. It reads the
  fresh gold marts, computes the 8 monitored features with the **same SQL semantics** as the local
  `FEATURE_SQL`, and appends a tidy history to `vitals_gold.monitoring.drift_report`
  (`split, feature, psi, band, is_alert, run_ts`). The PSI math lives in `vitals.drift` (numpy/pandas
  only, no duckdb) and is imported on-cluster from the bundle-synced `src/`, so the local monitor,
  the connect/parity path, and the job all run **one implementation**. Verified two ways: a unit test
  pins `build_report` to the committed `drift_report.json`, and `make drift-databricks` confirms all
  16 `(split.feature)` PSI values match the local monitor to 4 dp.
- **Free-Edition note:** serverless `spark_python_task` needs an `environment_key` (a serverless
  `environments` entry); a dedicated `py_env` (numpy/pandas) carries the drift task.

## Update (2026-06-30) — the open follow-up is closed: the job runs the full medallion

Bronze + silver are now **in the job**: a single `python_wheel_task` (`medallion_ingest`, the wheel's
`vitals-medallion` entry point) runs **generate → bronze Delta → silver Delta** with the PHI +
non-empty gates, wired **before** `gold_dbt → drift_monitor`. One scheduled serverless run now does
the whole medallion, no laptop. Verified `TERMINATED SUCCESS` (`bundle deploy && bundle run`):
`medallion_ingest` (bronze=28816, silver=27402) → `gold_dbt` (marts + tests) → `drift_monitor`.

- **One codebase, three homes.** `vitals/env.py` resolves two signals at call time — `VITALS_BRONZE_DIR`
  (writable NDJSON dir; the job passes `/tmp/vitals_bronze`) and `VITALS_SPARK_MODE` (`ambient` on-cluster
  vs `serverless` for connect). Defaults reproduce the original behaviour, so the connect dev path and
  the DuckDB clone-and-run default are untouched.
- **Single task by design.** generate/bronze/silver share one Python process + Spark session and hand
  data off in-process → one task; gold (dbt on a SQL warehouse) and drift (pandas, after gold) are
  different runtimes → separate tasks. Task boundaries follow the *runtime*, not the stage name.
- **In-job gates:** PHI (`assert_no_phi`) + non-empty counts hard-fail the task on-cluster; the dbt
  tests still gate gold. Cross-engine parity stays the dev/connect-time gate (no DuckDB on-cluster).
- **Three Free-Edition serverless lessons learned the hard way (each surfaced only on a live run):**
  1. **A wheel ships its dependencies.** With the MVP stack (duckdb/dbt/mlflow/…) in `[project]`
     dependencies, the wheel dragged all of it onto serverless and broke. Fix: core `vitals` is a lean
     library (empty required deps); the stack moved to a `local` extra — the on-cluster task needs only
     ambient Spark. *Separate the library from its app-runtime deps.*
  2. **Match the wheel's Python floor to the compute.** `requires-python>=3.12` won't install on
     serverless env version "2" (Python 3.11). Fix: pin `ingest_env` to client **"3"** (Python 3.12)
     rather than lowering the project floor (which broke local resolution).
  3. **dbt models must be dialect-correct on the target you actually run.** `metricflow_time_spine`
     used DuckDB `range(DATE,…)`; Spark's `range()` is integer-only. Fix: branch on `target.type`
     (DuckDB `range()` / Spark `sequence()+explode()`). ADR 0007 had called the semantic layer
     "Databricks-compatible but not exercised" — exercising it is what proved (and fixed) the gap.
- **Ambient Spark on serverless:** `DatabricksSession.builder.getOrCreate()` (no `.serverless()`)
  returns the ambient session inside the job; `WorkspaceClient().files.upload` writes the volume with
  ambient job auth. Both verified by the `medallion_ingest` SUCCESS.
