# ADR 0005 — databricks-connect for the Delta-on-UC writer, job-submit as the production path

**Status:** accepted · 2026-06-26

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
