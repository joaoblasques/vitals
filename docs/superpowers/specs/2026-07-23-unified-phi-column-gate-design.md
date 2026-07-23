# Design — Unified Safe Harbor PHI-column gate (all silver tables, both engines, CI-verified)

_Date: 2026-07-23 · Status: DRAFT — approved design, not yet implemented · Phase: compliance/DQ
governance (closes a real gap in the ADR 0002 PHI boundary, in the pattern of the 2026-07-14 salt fix)_

> **One-liner:** replace the two independently-maintained, 5-column PHI blocklists (DuckDB and
> Databricks paths) with **one shared module** covering all **18 HIPAA Safe Harbor identifier
> categories**, applied to **every silver table** (not just `patient`), and add a CI step that
> exercises the Databricks-path assertion — which today only runs on-cluster and is never checked
> by CI at all.

## Goal

ADR 0002 established silver as the de-identified PHI boundary and added a "build-time assertion
[that] fails the pipeline if any PHI column survives into silver." A research pass while evaluating
external healthcare-compliance subagents (VoltAgent's `hipaa-compliance.md`, `ajhcs/healthcare-agents`
— both prompt packs for admin/compliance workflows, not adopted; see [Provenance note](#provenance-note-why-no-external-tooling-was-adopted))
surfaced that this assertion, in practice, is:

- **Duplicated, not shared.** `src/vitals/lakehouse.py` (DuckDB) and
  `src/vitals/backends/databricks_delta.py` (Databricks) each hard-code their own column blocklist,
  and the two lists have already drifted (`{"name","identifier","address","birthDate"}` vs.
  `{"name","identifier","address","birthDate","ssn"}`).
- **Narrow.** Both lists cover roughly 4-5 of the 18 Safe Harbor identifier categories — mostly
  direct-name/address fields. Categories like telephone/fax/email, medical record or account
  numbers, device/vehicle identifiers, URLs, IP addresses, and biometric identifiers have no
  representation at all.
- **Checked on one table only.** Both assertions run against `silver.patient` exclusively. Silver
  actually has eight tables — `patient`, `target`, `observation`, `condition`, `note`, `claim`,
  `pro`, `wearable_daily` — and `note`/`claim` in particular are the tables most likely to carry a
  stray identifier column if a future source field is added carelessly.
- **Not CI-verified on the Databricks path.** `medallion_job.py` calls
  `assert_no_phi(silver_patient_columns())`, but that function only runs when the job executes
  on-cluster. CI's hermetic gate (`make build` → `make dq`) never touches it, so a regression in the
  Databricks blocklist could ship undetected.

This unit consolidates the two blocklists into one canonical, fully-enumerated Safe Harbor
identifier set; extends the check to every silver table on both engines; and adds a CI step that
proves the Databricks-path assertion still has teeth, without requiring a live cluster.

## Non-negotiable principles this serves / preserves

- **PHI boundary at silver.** This is a direct hardening of the project's signature invariant —
  same boundary, same "fail loud, not silent" posture, just complete instead of partial.
- **DQ gates before exposing data.** The new coverage is exercised in CI on every push/PR, same as
  the existing `make build` / `make dq` gates — nothing here is optional or skippable.
- **Reproducible from code.** One module, version-controlled, is the single source of truth for
  "what counts as PHI" — no drift between engines is possible once they both import it.

## Scope decisions (locked with the user)

- **Consolidate + extend to all silver tables** (the chosen option, over a minimal de-dup-only fix
  or a broader content-level regex scan).
- **Column-name/pattern matching only** — no content-level scanning of free-text fields (e.g.
  detecting an embedded SSN inside a `note.text` value). This is a real, explicitly acknowledged gap
  going forward (see [Non-goals](#non-goals-yagni)), not a silent omission.
- **New ADR 0011**, cross-referencing ADR 0002, rather than amending ADR 0002's History section —
  the scope (full 18-category coverage, both engines, CI wiring) is substantial enough to warrant
  its own decision record, matching the project's existing ADR granularity.

## Architecture

```
src/vitals/phi.py (new, shared, no engine imports)
  SAFE_HARBOR_PATTERNS: dict[str, set[str]]   # 18 categories -> column-name patterns
  assert_no_phi(table: str, columns: set[str]) -> None   # raises with category + column on hit

        │ imported by                              │ imported by
        ▼                                           ▼
src/vitals/lakehouse.py (DuckDB)          src/vitals/backends/databricks_delta.py (Databricks)
  after building EACH silver table:         assert_no_phi() called for EACH silver table's
  phi.assert_no_phi(table, cols)            columns, from build_silver() / medallion_job.py

CI (.github/workflows/ci.yml):
  existing "make build" step now exercises all 8 tables via lakehouse.py (DuckDB path, live)
  NEW step: unit-test-level check of databricks_delta.assert_no_phi() against the real Delta
            silver schemas (via silver_patient_columns()-style column introspection per table,
            no live cluster required — see Components #3)
```

## Components

### 1. `src/vitals/phi.py` (new) — the canonical Safe Harbor identifier set + the check

- `SAFE_HARBOR_PATTERNS: dict[str, set[str]]` — one entry per Safe Harbor category (names;
  geographic subdivisions smaller than state; dates except year; telephone numbers; fax numbers;
  email addresses; SSNs; medical record numbers; health plan beneficiary numbers; account numbers;
  certificate/license numbers; vehicle identifiers; device identifiers; URLs; IP addresses;
  biometric identifiers; full-face photos/images; other unique identifying numbers), each mapping to
  the column-name substrings/patterns that would indicate that category (e.g. `"telephone_fax"` →
  `{"phone", "telephone", "fax"}`). Deliberately column-*name*-based, matching the existing
  assertion's mechanism — not a new detection technique.
- `assert_no_phi(table: str, columns: set[str]) -> None` — checks `columns` against every category
  in `SAFE_HARBOR_PATTERNS`, raises `AssertionError(f"PHI leaked into silver.{table}: {col} matches
  Safe Harbor category '{category}'")` on any hit. Table name in the error (today's messages don't
  distinguish tables, since only `patient` was ever checked).
- Pure, hermetic, no `duckdb`/`pyspark` import — testable standalone.

### 2. `lakehouse.py` and `backends/databricks_delta.py` — call sites

- `lakehouse.py`: after each of the 8 `CREATE OR REPLACE TABLE silver.*` statements, call
  `phi.assert_no_phi(table_name, cols)` using the same `DESCRIBE silver.<table>` introspection the
  current single check already uses. Replaces the existing inline `assert` at line ~172.
- `databricks_delta.py`: replace the local `PHI_COLUMNS` set and `assert_no_phi()` with a thin
  wrapper that imports `phi.assert_no_phi` and calls it per-table. `silver_patient_columns()`
  generalizes to a per-table column-introspection helper (`silver_table_columns(name)`), called for
  all 8 tables from `medallion_job.py` instead of just `patient`.

### 3. CI (`.github/workflows/ci.yml`) — verifying the Databricks path without a cluster

- New step after the existing DQ gate: run `tests/test_phi.py`'s Databricks-path test, which feeds
  `databricks_delta.assert_no_phi` (or the generalized per-table wrapper) a **poisoned column list**
  (e.g. `{"patient_key", "phone_number"}`) and asserts it raises. This proves the Databricks call
  site is wired to the shared module correctly, without needing `pyspark`/a live cluster — it is a
  unit test of the assertion logic, not an integration test of the Databricks job.

## Data flow / gate

Unchanged shape from today, extended in coverage: `make build` builds all 8 silver tables and now
checks all 8 against the full 18-category set (previously: 1 table, ~5 categories) → any leak fails
the build immediately, same as now. `medallion_job.py` on Databricks does the same for all 8 tables
via the shared module. CI's existing `make build` step exercises the DuckDB path live; the new step
exercises the Databricks path's logic without a cluster.

## Error handling / gates

- **Any Safe Harbor pattern match on any silver table** → `AssertionError` naming the table, column,
  and category → build fails (DuckDB) or job fails (Databricks) — no change in failure posture, just
  broader coverage.
- **No behavior change for clean data** — the existing 8 silver tables' real schemas must continue
  to pass with zero false positives; this is verified in testing (below) against the actual current
  schemas before considering the gate complete.

## Testing

- **`tests/test_phi.py` (new, hermetic, no duckdb/pyspark needed for the pure parts):**
  - `SAFE_HARBOR_PATTERNS` covers all 18 categories (a test that fails if a category is silently
    dropped — mirrors the existing `expectations_spec()` coverage test in `test_dq.py`).
  - `assert_no_phi()` raises on a poisoned column list per category (parametrized: one case per
    category, confirming each pattern set actually catches its own category).
  - `assert_no_phi()` does **not** raise against each of the 8 real silver tables' current column
    sets (regression guard — proves the broadened check has zero false positives on real schemas
    today).
  - The Databricks-path CI check described in Components #3.
- **Live gate:** `make build` (DuckDB, all 8 tables) → exits 0 on the real pipeline, same as today.

## Docs

- New `docs/adr/0011-unified-phi-column-gate.md` — context (the drift + narrow coverage found
  during the healthcare-subagent research pass), decision (one shared module, full 18-category
  coverage, all silver tables, CI-verified on both engine paths), consequences (no engine can drift
  from the canonical list again; `note`/`claim` are now checked; content-level scanning remains a
  documented gap), cross-referencing ADR 0002.
- `docs/adr/0002-de-identification-at-silver.md` — a one-line History entry pointing to ADR 0011,
  consistent with how the salt-gap fix was logged.

### Provenance note (why no external tooling was adopted)

Three healthcare-domain resources were evaluated for this work: `ajhcs/healthcare-agents`,
VoltAgent's `healthcare-admin.md`, and VoltAgent's `hipaa-compliance.md`. All three are LLM prompt
packs targeting healthcare *administrative/compliance* workflows (revenue cycle, HEDIS, BAA
drafting) — not data-engineering tooling, and orthogonal to Vitals' actual PHI boundary mechanism.
`healthcare-admin.md` installs directly from the `ajhcs` repo (same source cited twice), which is a
single-author, low-star, self-graded project. None were installed or cited as project dependencies;
`hipaa-compliance.md`'s enumeration of the 18 Safe Harbor categories was used only as a prompt to
verify this design's category list against the standard, the same way any reference checklist would
be used.

## Non-goals (YAGNI)

- **No content-level scanning** of free-text columns (e.g. detecting an SSN embedded inside
  `note.text`). Column-name matching only, matching the existing mechanism. Documented as a known
  gap in ADR 0011; revisit if `note`/`claim` free-text fields are ever exposed beyond silver.
- **No new external dependency.** The Safe Harbor category set is a plain Python
  `dict[str, set[str]]` — no NLP/PII-detection library.
- **No change to the salt/date-shift/age-cap mechanisms** — those are already correct per ADR 0002;
  this unit is scoped to the column-presence check only.

## Files touched

| File | Change |
|---|---|
| `src/vitals/phi.py` | new — canonical 18-category Safe Harbor pattern set + `assert_no_phi()` |
| `src/vitals/lakehouse.py` | replace inline PHI `assert` with `phi.assert_no_phi()` calls for all 8 silver tables |
| `src/vitals/backends/databricks_delta.py` | replace local `PHI_COLUMNS`/`assert_no_phi` with a wrapper over `phi.assert_no_phi`; generalize `silver_patient_columns()` to per-table |
| `src/vitals/medallion_job.py` | call the per-table PHI check for all 8 silver tables, not just `patient` |
| `tests/test_phi.py` | new — category coverage test, per-category poison tests, real-schema false-positive guard, Databricks-path CI check |
| `.github/workflows/ci.yml` | new step exercising the Databricks-path assertion logic |
| `docs/adr/0011-unified-phi-column-gate.md` | new ADR |
| `docs/adr/0002-de-identification-at-silver.md` | one-line History entry pointing to ADR 0011 |
