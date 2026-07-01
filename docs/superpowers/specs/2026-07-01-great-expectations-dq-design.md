# Design — Great Expectations as the silver DQ contract (gated in CI)

_Date: 2026-07-01 · Status: DRAFT — approved design, not yet implemented · Phase: data-quality governance (makes "validate vocabularies as contracts, not vibes" real)_

> **One-liner:** turn the silver layer into a **gated** data-quality contract with **Great
> Expectations** — a code-defined expectation suite whose signature checks are **coded-vocabulary
> value-sets** (every `icd10_code` valid + 100% coded; glucose always mg/dL), plus the PHI-boundary and
> range/uniqueness expectations — and run it **in CI as a gate** so bad silver can't be exposed. dbt
> keeps gating gold; GE gates silver.

## Goal

The project's principles say: *"Coded-vocabulary data quality … validate vocabularies as DQ contracts
(Great Expectations), not vibes"* and *"DQ gates before exposing data — validate source + consumption
layers in CI."* Today the silver DQ (`lakehouse._dq_silver`) is a **descriptive report**
(`dq_report.json`) — metrics, not gates (only the PHI check actually fails the build); dbt tests gate
**gold**. Great Expectations is named in the stack but not wired. This unit makes it real: GE becomes
the **gating DQ contract for silver** (the consumption layer / PHI boundary), with the coded-vocabulary
validation as first-class expectations — the health-data DQ signature that separates this from generic
ETL.

## Non-negotiable principles this serves / preserves

- **DQ as CI gates, not vibes.** GE runs **in CI** and **fails the build** on any violated expectation
  — it can't be skipped. This is the key difference from the Feast/pgvector demos (optional, skip in CI):
  GE is a *gate*, so CI installs it and runs it.
- **Coded-vocabulary contracts.** The valid value-sets come from the version-controlled `vitals.vocab`
  maps — the same standards the silver conform uses (DRY).
- **PHI boundary at silver.** The de-id invariant (no HIPAA identifiers in `silver.patient`) is expressed
  as a GE expectation, formalizing today's ad-hoc assertion.
- **Clone-and-run stays lean.** GE is a `dq` optional extra; `make setup` / `make run` don't need it
  (the pipeline runs without GE). CI installs `--extra dq` and runs the gate.

## Scope decisions (locked with the user)

- **Silver only** (the consumption layer / PHI boundary). No bronze "source landed" suite (the mess is
  expected; lower value). dbt continues to gate gold.
- **CI-gating** (fails the build), not report-only.
- **Complement, don't replace** the existing descriptive `dq_report.json` (it feeds `results.json` /
  the website); GE adds the gate alongside it.

## Architecture

```
data/vitals.duckdb (silver.*)  ──read→pandas──►  GE ExpectationSuite (code-defined, vocab-derived)
                                                        │ validate
                                     success → exit 0   ▼   any failure → exit non-zero (gate)
                              writes data/ge_validation.json (per-expectation summary)
   CI:  uv sync --extra dq  →  make build (writes silver)  →  make dq  (the gate; can't be skipped)
```

## Components

### 1. `src/vitals/dq.py` (new) — the suite + the gate

Pure suite definition separated from GE validation I/O (mirrors the `vector_index`/`feature_store`
split):

- **Pure (hermetic-testable, no GE import):** `valid_icd10() -> set[str]` (from `vocab.ICD_DISPLAY`
  keys + `vocab.TEXT_TO_ICD` values), `VALID_METRICS = {"pain","adherence","heart_rate","glucose","other"}`,
  `PHI_COLUMNS = {"name","identifier","address","birthDate","ssn"}`, range constants (PRO 0–100, steps
  0–50000). `expectations_spec() -> list[dict]` — a plain, testable description of every expectation
  (table, column, kind, kwargs) that the GE layer materializes.
- **GE I/O (imports `great_expectations` inside the functions):** `is_available()`; `_read_silver() ->
  dict[str, pd.DataFrame]` (read the silver tables from DuckDB); `validate() -> dict` (build an ephemeral
  GE context, add the suite from `expectations_spec()`, validate each silver table's batch, aggregate
  into `{success: bool, results: [...], n_expectations, n_failed}`); `main()` (run `validate()`, write
  `data/ge_validation.json`, print a summary, **`sys.exit(1)` if not success**).

### 2. The expectation suite (silver)

| Table.column | Expectation | Source of truth |
|---|---|---|
| `condition.icd10_code` | not-null (100% coded) **and** valid ICD-10 | `vocab` (value-set or format+recovered-subset — see risk) |
| `observation.metric` | in `VALID_METRICS` | `dq.VALID_METRICS` |
| `observation.unit_std` (glucose) | == `mg/dL` | unit-standardization contract |
| `patient` columns | none of `PHI_COLUMNS` present | PHI boundary |
| `patient.patient_key` | unique + not-null | conformed key |
| `pro.score` | between 0 and 100 | range |
| `wearable_daily.steps` | between 0 and 50000 (nulls ok) | range |

### 3. `Makefile` + CI

- `make dq` — `PYTHONPATH=src ./.venv/bin/python -m vitals.dq` (needs `uv sync --extra dq` + a built
  silver from `make build`/`make run`).
- `.github/workflows/ci.yml` — install `--extra dq` and add a **DQ gate** step after `make build`:
  `make dq`. A failed expectation fails CI.

### 4. `pyproject.toml`

- New `dq = ["great-expectations>=1.0"]` extra. CI installs it; clone-and-run does not need it.

## Data flow / gate

`make build` writes silver into `data/vitals.duckdb` → `make dq` reads the silver tables into pandas,
runs the GE suite, writes `data/ge_validation.json`, and exits non-zero if any expectation fails. In CI
this sits right after the existing `make build` DQ step, so silver is validated on every push/PR.

## Error handling / gates

- **Violated expectation** → `validate()` returns `success=False` → `main()` exits non-zero → CI fails.
- **GE not installed** (clone-and-run without the extra) → `make dq` isn't part of `make run`; the
  pipeline is unaffected. `is_available()` guards any incidental import.
- The descriptive `dq_report.json` continues to be written by `lakehouse` (unchanged).

## Testing

- **Pure unit tests** (`tests/test_dq.py`, hermetic — no GE needed): `valid_icd10()` returns the
  vocab-derived set; `expectations_spec()` covers every table/column in the suite table above (guards
  against silently dropping an expectation).
- **GE "teeth" test** (`importorskip("great_expectations")`): feed `validate`'s core a small pandas
  DataFrame that violates an expectation (e.g. an out-of-set `icd10_code`, or a PHI column present) and
  assert `success is False` and the offending expectation is in the failures. Proves the gate actually
  catches violations (not a rubber stamp). Because CI installs `--extra dq`, this **runs in CI** (not
  skipped); a bare local `make test` without the `dq` extra skips it. The pure tests always run.
- **Live gate:** `make build` then `make dq` → exit 0, `ge_validation.json` shows all expectations
  passed on the real conformed silver.
- **Clone-and-run + hermetic pipeline:** `make build` + the existing suite stay green; `make run`
  doesn't require GE.

## Docs

- New ADR `docs/adr/0009-great-expectations-silver-dq.md` — GE as the **gating** silver DQ contract;
  coded-vocabulary value-sets from `vocab` as the signature; **gated in CI** (the deliberate difference
  from the optional-extra demos — a gate must run, not skip); complements the descriptive
  `dq_report.json`; dbt-gates-gold / GE-gates-silver division; local DuckDB silver (GE-on-Databricks out
  of scope).
- `README.md` — the "**Great Expectations**" stack item made true; note `make dq` + that CI gates silver.

## Non-goals (YAGNI)

- No GE on gold (dbt tests own that layer) or bronze (source mess is expected).
- No GE **Data Docs** HTML site — just the validation gate + a JSON result (YAGNI).
- No GE on Databricks/Delta silver (local DuckDB silver is the deliverable; same stance as MetricFlow /
  Feast staying local).
- No replacement of `dq_report.json` or the `lakehouse` DQ metrics.

## Risk to pin in the plan

- **GE API churn.** Great Expectations 1.x differs substantially from 0.18 (context/datasource/suite
  API). The plan pins the exact 1.x flow (ephemeral context → pandas batch → suite → validate → result)
  against current docs (via context7), like the Feast unit.
- **The valid ICD-10 value-set.** The suite must not be falsely strict. The plan **verifies the actual
  distinct `silver.condition.icd10_code` values** and picks the correct expectation form: an explicit
  **set membership** if silver only carries the known vocab codes (stronger, preferred), or **not-null +
  ICD-10 format regex** with a set-membership check on the *text-recovered* subset if Synthea emits a
  broader code set. Requirement: the vocabulary is validated (not vibes) without false failures.

## Files touched

| File | Change |
|---|---|
| `src/vitals/dq.py` | new — pure suite spec + GE validation gate + `main` |
| `Makefile` | `dq` target |
| `.github/workflows/ci.yml` | install `--extra dq`; add the `make dq` gate step |
| `pyproject.toml` | new `dq` optional extra (`great-expectations>=1.0`) |
| `tests/test_dq.py` | new — pure suite tests + GE "teeth" test |
| `docs/adr/0009-great-expectations-silver-dq.md` | new ADR |
| `README.md` | Great Expectations stack item made true + `make dq` note |
