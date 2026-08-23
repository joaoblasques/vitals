# ADR 0011 — Unified Safe Harbor PHI-column gate

**Status:** accepted · 2026-07-23

## Context
ADR 0002 established silver as the de-identified PHI boundary, with "a build-time assertion [that]
fails the pipeline if any PHI column survives into silver." In practice this assertion was:

- **Duplicated, not shared** — `src/vitals/lakehouse.py` (DuckDB) and
  `src/vitals/backends/databricks_delta.py` (Databricks) each hard-coded their own column
  blocklist, and the two had already drifted (`{"name","identifier","address","birthDate"}` vs.
  `{"name","identifier","address","birthDate","ssn"}`).
- **Narrow** — both covered roughly 4-5 of the 18 HIPAA Safe Harbor identifier categories (45 CFR
  164.514(b)(2)), mostly direct name/address fields.
- **Checked on one table only** — both ran against `silver.patient` exclusively, though silver has
  eight tables (`patient`, `target`, `observation`, `condition`, `note`, `claim`, `pro`,
  `wearable_daily`); `note` and `claim` are the tables most likely to carry a stray identifier if a
  future source field is added carelessly.
- **Not CI-verified on the Databricks path** — `medallion_job.py` calls the Databricks assertion,
  but that only runs on-cluster; CI's hermetic gate (`make build` + `make dq`) never exercised it.

## Decision
One shared module, `src/vitals/phi.py`, defines the full 18-category Safe Harbor pattern set
(`SAFE_HARBOR_PATTERNS`) and a single `assert_no_phi(table, columns)` check. Both engines import it
and call it for every silver table. The Databricks engine's check (`assert_all_silver_no_phi`) is
proven in CI with a fake `SparkSession`, without needing a live cluster — the module's I/O functions
are already lazy-imported, so the check itself is hermetically testable.

Column-name/pattern matching only, matching the mechanism ADR 0002 already established — this is
not a content-level scan of free-text values.

## Consequences
- No engine can drift from the canonical Safe Harbor list again — there is exactly one definition.
- `note` and `claim` (previously unchecked) now gate on PHI column names, same as `patient` always
  did.
- The Databricks-path assertion has a CI-covered regression guard for the first time.
- **Known gap, deliberately out of scope:** a column named innocuously (e.g. `notes`) that happens
  to contain an embedded SSN or name inside its free-text *values* is not caught — this remains a
  content-level scanning gap, not addressed here. Revisit if `note`/`claim` free-text fields are
  ever exposed beyond silver.

## History
- 2026-07-23 — accepted. Closes the gap found while evaluating external healthcare-compliance
  subagents (prompt packs, not adopted as project dependencies — see the design spec at
  `docs/superpowers/specs/2026-07-23-unified-phi-column-gate-design.md` for the provenance note).
