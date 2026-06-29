# Design — Hermetic CI quality gate (lint · tests · DQ contracts)

_Date: 2026-06-29 · Status: DONE — shipped `5dac932`, CI run `28370097570` green on the runner (26 dbt tests ran) · Phase: ops-hardening (follows the bundle alerts + drift-task unit)_

> **One-liner:** every push/PR runs the can't-skip checks — `ruff`, the pytest suite, and the local
> data pipeline whose PHI boundary + 26 dbt tests are hard gates — on DuckDB, with **no credentials**.
> Backs the project principle *"DQ as CI gates, not vibes — wired into GitHub Actions so checks can't
> be skipped."* The credentialed Databricks parity stays a documented manual `make` step.

## Goal

Close a real gap: the repo has 20 pytest tests, a `ruff` config, a local DuckDB pipeline with a PHI
assertion, and 26 dbt DQ tests — but **none of them run in CI**. Today only `terraform.yml` (fmt /
validate, no creds) and `docs.yml` (mkdocs deploy) exist. A Senior-DE showcase should not let code
merge without the quality gates executing. This unit adds a hermetic `CI` workflow that enforces
them on every push and pull request.

## Non-negotiable principles this serves

- **DQ gates before exposing data** — the PHI boundary and dbt DQ contracts execute in CI, can't be
  skipped.
- **Reproducible from code** — `uv.lock` is committed; CI installs from the lock, pinned actions.
- **Never commit secrets** — public repo. The gate is **hermetic** (DuckDB only, no Databricks creds),
  matching the existing `terraform.yml` "init, no backend, no creds" philosophy.

## Scope decisions (locked with the user)

1. **Gate depth:** lint + unit tests + the **local DQ/dbt** pipeline (generate → silver → `dbt build`).
   *Not* the ML serve step (heavier, less about data quality), *not* lint-only.
2. **Databricks parity:** **out of push/PR CI.** It needs live creds + the Free-Edition demo
   workspace, so it stays the existing manual `make bronze-/silver-/gold-/drift-databricks` targets.
   No GitHub secrets, no demo-workspace hits on every push.

## Current state

| Asset | Exists today | Runs in CI today |
|---|---|---|
| `ruff` (line-length 100, py312) | yes (`pyproject.toml`) | **no** |
| 20 pytest tests (`tests/`) | yes (pure/local, no workspace) | **no** |
| Local pipeline `make run` (generate→silver→dbt→serve) | yes | **no** |
| PHI boundary assertion | `lakehouse.build()` raises on leak | **no** |
| 26 dbt DQ tests | run via `_dbt_build(check=True)` | **no** |
| `terraform.yml`, `docs.yml` | yes | yes |

**Blocker for `ruff check .`:** `src/vitals/run.py` has 4 pre-existing `E702` (semicolon) violations.
They must be fixed for the lint gate to pass — fixed as part of this unit since the lines are touched
anyway by the `--no-serve` refactor.

## Components

### 1. `.github/workflows/ci.yml` (new)

Mirrors the repo's established workflow style: path-filtered triggers, least privilege, pinned actions.

- **name:** `CI`
- **on:** `push` (branches `[main]`), `pull_request`, `workflow_dispatch`; path-filtered to code:
  `src/**`, `dbt/**`, `tests/**`, `pipelines/**`, `pyproject.toml`, `uv.lock`, `Makefile`,
  `mise.toml`, `.github/workflows/ci.yml`. (Doc-only / infra-only changes have their own workflows.)
- **permissions:** `contents: read`
- **one job `gate`** on `ubuntu-latest`, steps:
  1. `actions/checkout@v4`
  2. `astral-sh/setup-uv@v5` with caching (key derived from `uv.lock`)
  3. `uv sync --extra dev` (provisions Python 3.12 + the MVP stack + ruff/pytest from the lock)
  4. `ruff check .`
  5. `uv run pytest -q`
  6. `make build` — the hermetic data pipeline (generate → silver → `dbt build`)

Single job, sequential steps: fast enough (~2–4 min) and clearer than parallelizing lint/test.

### 2. `src/vitals/run.py` (edit)

- Add a `--no-serve` flag: `python -m vitals.run --no-serve` runs steps 1–3 (generate, silver, dbt
  build) and skips step 4 (serve: MLflow model + RAG). Default (no flag) is unchanged — full slice.
- Fix the 4 `E702` semicolons (split the `print(...); call()` lines) so `ruff check .` passes.
- Keep it minimal: argument parsing is a single `--no-serve` check, not a full argparse subcommand
  tree (YAGNI).

### 3. `Makefile` (edit)

- Add a `build` target: `PYTHONPATH=src ./.venv/bin/python -m vitals.run --no-serve`. This is what CI
  calls; humans can run it too. The `.venv` it references is exactly what `uv sync` creates
  (`UV_PROJECT_ENVIRONMENT=.venv`), and `dbt-duckdb` is a core dep so `.venv/bin/dbt` is present.
- Add `build` to `.PHONY`.

### 4. `README.md` (edit)

- Add a CI status badge next to any existing badges (small showcase signal that the gate is real).

## Data flow (CI run)

```
push/PR ──> setup-uv (cached) ──> uv sync --extra dev
   ├─ ruff check .                         # style/lint gate
   ├─ uv run pytest -q                     # 20 unit tests (PSI math, parity logic, PHI assert logic)
   └─ make build = vitals.run --no-serve
        ├─ generate.generate()             # bronze NDJSON
        ├─ lakehouse.build()               # silver — RAISES if PHI leaks (hard gate)
        └─ dbt build (check=True)          # 26 dbt DQ tests — non-zero exit fails CI (hard gate)
```

Any failing step fails the job; a red gate blocks merge.

## Error handling / gates

- **Lint:** `ruff check .` non-zero → fail.
- **Tests:** pytest non-zero → fail.
- **PHI boundary:** `lakehouse.build()` `assert` → AssertionError → non-zero → fail. (The project's
  signature invariant, now enforced in CI.)
- **DQ contracts:** `dbt build` runs the 26 tests with `check=True`; any failure → non-zero → fail.

## Testing / verification

- **Before pushing:** run the exact CI commands locally and confirm green:
  `uv sync --extra dev` · `ruff check .` · `uv run pytest -q` · `make build`.
- **The workflow itself** is proven by the first push to a branch / PR going green (and by
  intentionally confirming it *would* go red if a dbt test or the PHI assert failed — reasoned, not a
  destructive test).

## Non-goals (YAGNI)

- No Databricks parity in push/PR CI (kept as manual `make` targets).
- No ML-serve step in the gate (heavier, not a DQ contract).
- No Python version matrix (single 3.12, pinned in `mise.toml`).
- No separate lint/test jobs or caching beyond `setup-uv`'s lockfile cache.
- No coverage gate / no new test frameworks.

## Files touched

| File | Change |
|---|---|
| `.github/workflows/ci.yml` | new — the hermetic gate |
| `src/vitals/run.py` | `--no-serve` flag + fix 4 `E702` |
| `Makefile` | `build` target (+ `.PHONY`) |
| `README.md` | CI status badge |
