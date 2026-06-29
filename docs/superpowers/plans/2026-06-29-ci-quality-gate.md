# CI Quality Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a hermetic GitHub Actions `CI` workflow that runs `ruff`, the pytest suite, and the local data pipeline (PHI-boundary assertion + 26 dbt DQ tests) on every push/PR — no credentials.

**Architecture:** A new `.github/workflows/ci.yml` installs deps from `uv.lock` and runs lint → tests → `make build`. `make build` calls a new data-only mode of the existing entrypoint (`python -m vitals.run --no-serve`) that runs generate → silver → dbt build but skips the heavier ML serve step. The PHI assertion in `lakehouse.build()` and the 26 dbt tests (`dbt build`, `check=True`) are the hard gates.

**Tech Stack:** GitHub Actions, `astral-sh/setup-uv`, uv, ruff, pytest, dbt-duckdb, DuckDB.

## Global Constraints

- Python **3.12** only (pinned in `mise.toml`); no version matrix.
- **Hermetic** — DuckDB only, **no Databricks credentials / no secrets** (public repo).
- Install from the committed **`uv.lock`** (`uv sync`), pinned action versions.
- ruff config: `line-length = 100`, `target-version = py312` (from `pyproject.toml`).
- Match existing workflow style (`terraform.yml`, `docs.yml`): path-filtered triggers, `permissions: contents: read`, `workflow_dispatch`.
- Repo slug for badges/URLs: `joaoblasques/vitals`.
- Databricks parity is **out of scope** for CI (stays the manual `make *-databricks` targets).

---

### Task 1: Data-only pipeline mode (`run.py --no-serve`) + `make build`

Add a `--no-serve` flag to the single entrypoint so CI can run the data pipeline (generate → silver → dbt) without the ML serve step, and expose it as `make build`. Also fixes the 4 pre-existing `E702` semicolon lint violations in `run.py` (required for the `ruff check .` gate; the lines are rewritten here anyway).

**Files:**
- Modify: `src/vitals/run.py` (`main()` → `main(argv)`, lines 22-33)
- Modify: `Makefile` (`.PHONY` line + add `build` target)
- Test: `tests/test_run.py` (create)

**Interfaces:**
- Produces: `vitals.run.main(argv: list[str] | None = None) -> None` — runs generate → silver → dbt; runs serve unless `"--no-serve"` is in `argv`. `argv=None` reads `sys.argv[1:]`.
- Produces: `make build` — runs `python -m vitals.run --no-serve` via `.venv`.
- Consumes (unchanged): `vitals.generate.generate()`, `vitals.lakehouse.build()`, `vitals.run._dbt_build()`, `vitals.serve.run()`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_run.py`:

```python
"""Unit tests for the run entrypoint's step selection (no real pipeline I/O).

The pipeline functions are monkeypatched to record-only stubs, so we test that `--no-serve`
runs generate -> silver -> dbt and skips serve, while the default runs all four.
"""
from vitals import run


def _patch(monkeypatch, called):
    monkeypatch.setattr("vitals.generate.generate", lambda: called.append("generate"))
    monkeypatch.setattr("vitals.lakehouse.build", lambda: called.append("silver"))
    monkeypatch.setattr("vitals.run._dbt_build", lambda: called.append("dbt"))
    monkeypatch.setattr("vitals.serve.run", lambda: called.append("serve"))


def test_no_serve_runs_data_steps_only(monkeypatch):
    called = []
    _patch(monkeypatch, called)
    run.main(["--no-serve"])
    assert called == ["generate", "silver", "dbt"]


def test_default_runs_all_four_steps(monkeypatch):
    called = []
    _patch(monkeypatch, called)
    run.main([])
    assert called == ["generate", "silver", "dbt", "serve"]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --extra dev pytest tests/test_run.py -q`
Expected: FAIL — `main()` takes no positional arg (`TypeError: main() takes 0 positional arguments but 1 was given`).

- [ ] **Step 3: Refactor `run.py` `main()`**

Replace `main()` and the `__main__` block (current lines 22-33) with:

```python
def main(argv: list[str] | None = None) -> None:
    argv = sys.argv[1:] if argv is None else argv
    run_serve = "--no-serve" not in argv

    from vitals import generate, lakehouse, serve

    print("\n[1/4] generate bronze ...")
    generate.generate()
    print("\n[2/4] bronze -> silver ...")
    lakehouse.build()
    print("\n[3/4] dbt: silver -> gold ...")
    _dbt_build()
    if run_serve:
        print("\n[4/4] serve: features + vectors + model ...")
        serve.run()
    print("\n✅ Vitals MVP slice complete. See data/results.json.")


if __name__ == "__main__":
    sys.exit(main())
```

Also update the module docstring's first line (line 1) to note the flag:

```python
"""Run the Vitals MVP slice locally: generate -> bronze/silver -> dbt gold -> serve.
```

becomes

```python
"""Run the Vitals MVP slice locally: generate -> bronze/silver -> dbt gold -> serve.

Use `--no-serve` to run the data pipeline only (generate -> silver -> dbt gold + DQ tests),
skipping the ML serve step — that is the hermetic gate `make build` / CI runs.
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --extra dev pytest tests/test_run.py -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Add the `make build` target**

In `Makefile`, add `build` to the `.PHONY` line (after `dbt`):

```make
.PHONY: setup run build dbt clean test dbcxn-setup bronze-databricks silver-databricks gold-baseline gold-databricks drift-databricks bundle-deploy bundle-run
```

And add the target immediately after the `run:` target block:

```make
build:          ## hermetic data gate: generate -> silver (PHI boundary) -> dbt gold + 26 DQ tests; no ML serve
	PYTHONPATH=src ./.venv/bin/python -m vitals.run --no-serve
```

- [ ] **Step 6: Verify `make build` runs green locally**

Run: `make build`
Expected: prints `[1/4] generate bronze`, `[2/4] bronze -> silver`, `[3/4] dbt: silver -> gold`, dbt build completes with `PASS=26` (the 26 dbt tests), and the run exits 0. It must NOT print `[4/4] serve`.

- [ ] **Step 7: Verify lint is now clean (E702 gone)**

Run: `uv run ruff check src/vitals/run.py`
Expected: `All checks passed!` (the 4 E702 violations are gone).

- [ ] **Step 8: Commit**

```bash
git add src/vitals/run.py Makefile tests/test_run.py
git commit -m "feat(run): --no-serve data-only mode + make build (CI gate) — fixes run.py E702

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01F1hdqzju3WbgEALYreWUjk"
```

---

### Task 2: CI workflow + README badge

Add the hermetic `CI` workflow and a status badge. Verify locally with the exact CI commands, then push and confirm the Actions run is green.

**Files:**
- Create: `.github/workflows/ci.yml`
- Modify: `README.md` (add badge after the tagline, line 3)

**Interfaces:**
- Consumes: `make build` and the lint/test commands from Task 1.
- Produces: a green `CI` check on push/PR.

- [ ] **Step 1: Create `.github/workflows/ci.yml`**

```yaml
name: CI

# Hermetic quality gate — runs on every push/PR with NO credentials (DuckDB only). The credentialed
# Databricks parity stays a manual step (make bronze-/silver-/gold-/drift-databricks).
on:
  push:
    branches: [main]
    paths:
      - "src/**"
      - "dbt/**"
      - "tests/**"
      - "pipelines/**"
      - "pyproject.toml"
      - "uv.lock"
      - "Makefile"
      - "mise.toml"
      - ".github/workflows/ci.yml"
  pull_request:
    paths:
      - "src/**"
      - "dbt/**"
      - "tests/**"
      - "pipelines/**"
      - "pyproject.toml"
      - "uv.lock"
      - "Makefile"
      - "mise.toml"
      - ".github/workflows/ci.yml"
  workflow_dispatch:

permissions:
  contents: read

jobs:
  gate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Set up uv (with lockfile cache)
        uses: astral-sh/setup-uv@v5
        with:
          enable-cache: true
          cache-dependency-glob: "uv.lock"
      - name: Install deps (from lock)
        run: uv sync --extra dev
      - name: Lint (ruff)
        run: uv run ruff check .
      - name: Unit tests (pytest)
        run: uv run pytest -q
      - name: DQ gate — local pipeline (PHI boundary + 26 dbt tests)
        run: make build
```

- [ ] **Step 2: Validate the workflow YAML parses**

Run: `uv run python -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml')); print('yaml ok')"`
Expected: `yaml ok` (no exception). If PyYAML is absent, run `uv run --with pyyaml python -c "..."` instead.

- [ ] **Step 3: Add the CI badge to `README.md`**

Insert directly below the tagline line (`**From raw clinical signals to trusted, AI-ready data.**`, line 3) — add a blank line then:

```markdown
[![CI](https://github.com/joaoblasques/vitals/actions/workflows/ci.yml/badge.svg)](https://github.com/joaoblasques/vitals/actions/workflows/ci.yml)
```

- [ ] **Step 4: Run the full CI command set locally (pre-push proof)**

Run each and confirm green:
```bash
uv sync --extra dev
uv run ruff check .
uv run pytest -q
make build
```
Expected: `All checks passed!` (ruff), `22 passed` (pytest — 20 existing + 2 new from Task 1), and `make build` exits 0 with `PASS=26` dbt tests and no `[4/4] serve` line.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/ci.yml README.md
git commit -m "ci: hermetic quality gate (ruff + pytest + local DQ/dbt) on push/PR

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01F1hdqzju3WbgEALYreWUjk"
```

- [ ] **Step 6: Push and verify the Actions run is green**

```bash
git push origin main
```
Then watch the run:
```bash
gh run watch "$(gh run list --workflow=ci.yml --limit=1 --json databaseId --jq '.[0].databaseId')" --exit-status
```
Expected: the `CI` run completes with conclusion **success**. If it fails, read the failing step's log (`gh run view --log-failed`), fix, and re-push.

---

## Self-Review

**Spec coverage:**
- Hermetic gate (lint + tests + DQ/dbt), no creds → Task 2 workflow + Task 1 `make build`. ✓
- `ci.yml` style (path filters, `contents: read`, `workflow_dispatch`, pinned actions) → Task 2 Step 1. ✓
- `run.py --no-serve` + fix 4 E702 → Task 1 Steps 3, 7. ✓
- `Makefile build` target → Task 1 Step 5. ✓
- README badge → Task 2 Step 3. ✓
- Hard gates (PHI assert + 26 dbt tests) → exercised by `make build` (Task 1 Step 6, Task 2 Step 4). ✓
- Databricks parity out of scope → no task adds it; comment in `ci.yml` notes it. ✓
- Verification = run exact commands locally + first push green → Task 2 Steps 4, 6. ✓

**Placeholder scan:** none — all code/commands are concrete.

**Type consistency:** `main(argv: list[str] | None = None)` defined in Task 1 and called as `run.main([...])` in its tests; `make build` defined in Task 1 and consumed in Task 2 — consistent.
