# Full-medallion `python_wheel_task` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Promote generate + bronze + silver into the deployed Asset Bundle job as one `python_wheel_task`, so a single scheduled serverless run does the whole medallion (generate → bronze → silver → gold → drift), unattended.

**Architecture:** A small `env.py` resolves two environment signals at call time (`VITALS_BRONZE_DIR`, `VITALS_SPARK_MODE`) so the same code serves local DuckDB, databricks-connect, and on-cluster — defaults unchanged. A new `medallion_job.py` is the wheel entry point (generate → land_bronze → build_silver → PHI + non-empty gates). The bundle builds a wheel artifact and runs it as `medallion_ingest`, wired before the existing `gold_dbt → drift_monitor`.

**Tech Stack:** Python (stdlib generator), PySpark via databricks-connect (dev) / ambient serverless (on-cluster), Databricks Asset Bundles, `python_wheel_task`, setuptools + `uv build`, pytest.

## Global Constraints

- **Defaults unchanged.** `env.bronze_dir()` defaults to `<repo>/data/bronze`; `env.spark_mode()` defaults to `"serverless"`. The local DuckDB clone-and-run path, the databricks-connect dev path (`make bronze-/silver-databricks`), and CI must behave exactly as before.
- **Two env signals only:** `VITALS_BRONZE_DIR` (writable NDJSON dir; the bundle sets `/tmp/vitals_bronze`) and `VITALS_SPARK_MODE` (`ambient` on-cluster, set by the wheel entry point; `serverless` otherwise).
- **Generate stays pure-stdlib** — no new third-party dependency added to `generate.py`.
- **In-job gates:** `assert_no_phi(silver columns)` + non-empty bronze/silver counts. Both hard-fail (raise) → the task fails → the existing `on_failure` alert fires. No cross-engine parity in-job.
- **Single `medallion_ingest` task** for generate + bronze + silver (one PySpark substrate, in-process handoff). `gold_dbt` (dbt/warehouse) and `drift_monitor` (pandas, after gold) stay separate tasks.
- **Acceptance bar:** a real `databricks bundle deploy && databricks bundle run vitals_medallion` reaching `TERMINATED SUCCESS` (needs `source infra/terraform/.env`).
- **Wheel build:** setuptools backend + `uv build --wheel`; entry point `vitals-medallion = vitals.medallion_job:main`. Build artifacts (`dist/`, `build/`, `*.egg-info`) are gitignored (public repo).
- Tests import `vitals` via `[tool.pytest.ini_options] pythonpath = ["src"]`; new unit tests are hermetic (no Databricks).

---

### Task 1: environment-aware I/O — `env.py` + wire `generate` and the Databricks backend

Make the file location and the Spark accessor resolve from env signals at call time, with defaults that leave every existing path untouched.

**Files:**
- Create: `src/vitals/env.py`
- Modify: `src/vitals/generate.py` (replace the `BRONZE` constant with `env.bronze_dir()`)
- Modify: `src/vitals/backends/databricks_delta.py` (`BRONZE_DIR` → `env.bronze_dir()`; branch `_spark()` on `env.spark_mode()`)
- Test: `tests/test_env.py`

**Interfaces:**
- Produces: `vitals.env.bronze_dir() -> pathlib.Path`, `vitals.env.spark_mode() -> str` (`"serverless"` | `"ambient"`).

- [ ] **Step 1: Write the failing test for `env.py`**

Create `tests/test_env.py`:

```python
from pathlib import Path

from vitals import env


def test_bronze_dir_defaults_to_repo_data_bronze(monkeypatch):
    monkeypatch.delenv("VITALS_BRONZE_DIR", raising=False)
    assert env.bronze_dir() == Path(env.__file__).resolve().parents[2] / "data" / "bronze"


def test_bronze_dir_honors_override(monkeypatch, tmp_path):
    monkeypatch.setenv("VITALS_BRONZE_DIR", str(tmp_path))
    assert env.bronze_dir() == tmp_path


def test_spark_mode_defaults_to_serverless(monkeypatch):
    monkeypatch.delenv("VITALS_SPARK_MODE", raising=False)
    assert env.spark_mode() == "serverless"


def test_spark_mode_ambient_when_set(monkeypatch):
    monkeypatch.setenv("VITALS_SPARK_MODE", "ambient")
    assert env.spark_mode() == "ambient"
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run --extra dev pytest tests/test_env.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'vitals.env'`.

- [ ] **Step 3: Create `src/vitals/env.py`**

```python
"""Call-time resolution of the two environment signals that let one codebase serve three homes:
local DuckDB, databricks-connect (laptop drives remote Spark), and on-cluster serverless. Resolved
at call time (not import) so the wheel entry point can set them before use, and so they're testable.
Defaults reproduce the original behaviour exactly — see docs/adr/0005."""
from __future__ import annotations

import os
from pathlib import Path

_DEFAULT_BRONZE = Path(__file__).resolve().parents[2] / "data" / "bronze"


def bronze_dir() -> Path:
    """Directory for raw NDJSON. Default = repo data/bronze (local + connect unchanged); the bundle's
    medallion task overrides it to a writable /tmp dir via VITALS_BRONZE_DIR."""
    return Path(os.environ.get("VITALS_BRONZE_DIR", str(_DEFAULT_BRONZE)))


def spark_mode() -> str:
    """'ambient' when running ON Databricks compute (the wheel entry point sets it); 'serverless' for
    the databricks-connect dev path (laptop drives remote serverless). Default keeps connect unchanged."""
    return os.environ.get("VITALS_SPARK_MODE", "serverless")
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run --extra dev pytest tests/test_env.py -q`
Expected: PASS (4 tests).

- [ ] **Step 5: Wire `generate.py` to `env.bronze_dir()`**

In `src/vitals/generate.py`: remove the `BRONZE = Path(__file__).resolve().parents[2] / "data" / "bronze"` constant. Add `from vitals import env` to the imports. In `generate()`, replace `BRONZE.mkdir(parents=True, exist_ok=True)` with:

```python
    out = env.bronze_dir()
    out.mkdir(parents=True, exist_ok=True)
```

And change `_write` to resolve the dir at call time:

```python
def _write(name: str, rows: list[dict]) -> None:
    p = env.bronze_dir() / f"{name}.ndjson"
    with p.open("w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
```

Then confirm no other references to the old constant remain:
Run: `rg -n "\bBRONZE\b" src/vitals/generate.py`
Expected: no matches (all replaced).

- [ ] **Step 6: Add the generate-override test**

Append to `tests/test_env.py`:

```python
def test_generate_writes_to_override_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("VITALS_BRONZE_DIR", str(tmp_path))
    from vitals import generate
    generate.generate()
    assert (tmp_path / "patients.ndjson").exists()
    assert (tmp_path / "patients.ndjson").stat().st_size > 0
```

Run: `uv run --extra dev pytest tests/test_env.py -q`
Expected: PASS (5 tests). (If the file is named differently than `patients.ndjson`, adjust the assertion to a source name that `generate()` actually emits — list them with `rg -n "_write\(" src/vitals/generate.py`.)

- [ ] **Step 7: Wire `databricks_delta.py` to the env signals**

In `src/vitals/backends/databricks_delta.py`:
- Add `from vitals import env` to the imports.
- In `_upload_landing()` and `local_counts()`, replace `BRONZE_DIR / f"{name}.ndjson"` with `env.bronze_dir() / f"{name}.ndjson"`. (Leave the `BRONZE_DIR` module constant in place if other code references it; otherwise remove it. Check with `rg -n "BRONZE_DIR" src/`.)
- Replace `_spark()` with the mode branch:

```python
def _spark():
    if env.spark_mode() == "ambient":
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()        # ON Databricks: ambient serverless session
    from databricks.connect import DatabricksSession
    return DatabricksSession.builder.serverless().getOrCreate()  # connect from laptop (unchanged)
```

- [ ] **Step 8: Verify the local path is unaffected + full suite green**

Run: `uv run --extra dev pytest tests/ -q`
Expected: PASS / same skips as before (no regressions).
Run: `make build`
Expected: the local DuckDB pipeline completes (generate → silver → dbt gold + tests) exactly as before — proves the `generate`/`env` change didn't alter the default behaviour.

- [ ] **Step 9: Commit**

```bash
git add src/vitals/env.py src/vitals/generate.py src/vitals/backends/databricks_delta.py tests/test_env.py
git commit -m "feat(env): call-time bronze_dir + spark_mode signals; wire generate + databricks backend

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01F1hdqzju3WbgEALYreWUjk"
```

---

### Task 2: `medallion_job.py` entry point + wheel-buildable `pyproject`

The on-cluster entry point that sequences the medallion and enforces the gates, plus the packaging that lets the bundle ship it as a wheel.

**Files:**
- Create: `src/vitals/medallion_job.py`
- Modify: `pyproject.toml` (add `[build-system]` + `[project.scripts]`)
- Modify: `.gitignore` (ignore build artifacts)
- Test: `tests/test_medallion_job.py`

**Interfaces:**
- Consumes: `vitals.generate.generate`, `vitals.backends.databricks_delta.{land_bronze, build_silver, silver_patient_columns, assert_no_phi}` (existing).
- Produces: `vitals.medallion_job.main(argv: list[str] | None = None) -> None`; `vitals.medallion_job._assert_nonempty(bronze: dict[str,int], silver: dict[str,int]) -> None`; console script `vitals-medallion`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_medallion_job.py`:

```python
import pytest

from vitals import medallion_job


def test_assert_nonempty_passes_when_all_positive():
    medallion_job._assert_nonempty({"a": 1}, {"b": 2})  # no raise


def test_assert_nonempty_raises_on_zero():
    with pytest.raises(AssertionError) as e:
        medallion_job._assert_nonempty({"a": 1, "b": 0}, {"c": 3})
    assert "b" in str(e.value)


def test_main_sets_env_and_runs_stages_in_order(monkeypatch):
    calls = []
    monkeypatch.setattr("vitals.generate.generate", lambda: calls.append("generate") or {})
    import vitals.backends.databricks_delta as dx
    monkeypatch.setattr(dx, "land_bronze", lambda: (calls.append("land_bronze") or {"patients": 5}))
    monkeypatch.setattr(dx, "build_silver", lambda: (calls.append("build_silver") or {"patient": 5}))
    monkeypatch.setattr(dx, "silver_patient_columns", lambda: (calls.append("cols") or ["patient_key"]))
    monkeypatch.setattr(dx, "assert_no_phi", lambda cols: calls.append("assert_no_phi"))

    import os
    monkeypatch.delenv("VITALS_BRONZE_DIR", raising=False)
    monkeypatch.delenv("VITALS_SPARK_MODE", raising=False)
    medallion_job.main(["/tmp/vitals_bronze"])

    assert os.environ["VITALS_BRONZE_DIR"] == "/tmp/vitals_bronze"
    assert os.environ["VITALS_SPARK_MODE"] == "ambient"
    assert calls == ["generate", "land_bronze", "build_silver", "cols", "assert_no_phi"]
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --extra dev pytest tests/test_medallion_job.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'vitals.medallion_job'`.

- [ ] **Step 3: Create `src/vitals/medallion_job.py`**

```python
"""On-cluster full-medallion entry point — the bundle's python_wheel_task (`vitals-medallion`).
Generate synthetic data, land bronze Delta, build silver Delta, enforce the PHI + non-empty gates.
Runs ONLY on Databricks serverless: sets VITALS_SPARK_MODE=ambient so the shared backend grabs the
ambient session; the writable bronze dir arrives as the wheel parameter argv[0]. See docs/adr/0005."""
from __future__ import annotations

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
    bronze = dx.land_bronze()                       # upload to volume + write Delta
    silver = dx.build_silver()                      # bronze Delta -> de-identified silver Delta
    dx.assert_no_phi(dx.silver_patient_columns())   # PHI boundary — hard gate
    _assert_nonempty(bronze, silver)                # non-empty — hard gate
    print(f"✅ medallion ingest complete: bronze={sum(bronze.values())} silver={sum(silver.values())}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --extra dev pytest tests/test_medallion_job.py -q`
Expected: PASS (3 tests).

- [ ] **Step 5: Make `pyproject.toml` wheel-buildable + declare the entry point**

Add a `[build-system]` block (place it right after the `[project.optional-dependencies]`/before `[tool.setuptools...]`, anywhere top-level is fine) and a `[project.scripts]` block:

```toml
[build-system]
requires = ["setuptools>=61"]
build-backend = "setuptools.build_meta"

[project.scripts]
vitals-medallion = "vitals.medallion_job:main"
```

- [ ] **Step 6: Ignore build artifacts**

Add to `.gitignore`:

```
# Python build artifacts (wheel build for the Databricks bundle)
/dist/
/build/
*.egg-info/
```

- [ ] **Step 7: Verify the package still installs + the suite is green after adding the build backend**

Adding `[build-system]` makes `uv` build/install the project (it was a virtual project before). Confirm nothing breaks:
Run: `uv sync --extra dev --extra metrics && uv run --extra dev pytest tests/ -q`
Expected: PASS / same skips as before. (If `uv sync` errors on the build, the most likely cause is the setuptools package discovery — confirm `[tool.setuptools.packages.find] where = ["src"]` is present, which it is.)

- [ ] **Step 8: Verify the wheel builds and registers the entry point**

```bash
uv build --wheel
ls dist/vitals-0.0.1-*.whl
python -c "import zipfile,glob; w=glob.glob('dist/vitals-0.0.1-*.whl')[0]; \
print([n for n in zipfile.ZipFile(w).namelist() if 'entry_points' in n]); \
print(zipfile.ZipFile(w).read([n for n in zipfile.ZipFile(w).namelist() if n.endswith('entry_points.txt')][0]).decode())"
```
Expected: a `dist/vitals-0.0.1-py3-none-any.whl` exists; the printed `entry_points.txt` contains `vitals-medallion = vitals.medallion_job:main`. (`dist/` is gitignored, so it won't be committed.)

- [ ] **Step 9: Commit**

```bash
git add src/vitals/medallion_job.py pyproject.toml .gitignore tests/test_medallion_job.py uv.lock
git commit -m "feat(job): vitals-medallion wheel entry point + setuptools build backend

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01F1hdqzju3WbgEALYreWUjk"
```

---

### Task 3: bundle wheel artifact + `medallion_ingest` task

Add the wheel artifact and the task to the bundle, wired before `gold_dbt`, and validate the config.

**Files:**
- Modify: `databricks.yml`

**Interfaces:**
- Consumes: the wheel built from `pyproject` (Task 2), entry point `vitals-medallion`.

- [ ] **Step 1: Add the wheel artifact (top-level, after the `variables:` block)**

```yaml
# Build the project wheel so the medallion task can ship bronze/silver code to the job (ADR 0005).
artifacts:
  vitals_wheel:
    type: whl
    build: uv build --wheel
```

- [ ] **Step 2: Add a serverless environment carrying the wheel**

In `resources.jobs.vitals_medallion.environments`, add a third entry alongside `dbt_env`/`py_env`:

```yaml
        - environment_key: ingest_env
          spec:
            client: "2"
            dependencies:
              - ./dist/*.whl
```

(`./dist/*.whl` references the artifact built in Step 1 — DAB uploads it and installs it into the serverless environment. This is the Free-Edition-serverless wheel-attachment detail ADR 0005 flagged; `bundle validate` in Step 5 and the live deploy in Task 4 prove it. If `validate`/`deploy` rejects the glob, switch to the explicit artifact path form per current DAB docs — verify against https://docs.databricks.com/dev-tools/bundles/ and the `databricks bundle` schema.)

- [ ] **Step 3: Add the `medallion_ingest` task (first in the `tasks:` list)**

```yaml
        # Ingest: generate synthetic data, land bronze Delta, build silver Delta (PHI + non-empty
        # gates) — the whole pre-gold medallion as one packaged task (the wheel's vitals-medallion
        # entry point). Runs on serverless; the writable bronze dir is passed as a parameter.
        - task_key: medallion_ingest
          environment_key: ingest_env
          python_wheel_task:
            package_name: vitals
            entry_point: vitals-medallion
            parameters:
              - /tmp/vitals_bronze
```

- [ ] **Step 4: Make `gold_dbt` depend on the ingest task**

Add a `depends_on` to the existing `gold_dbt` task (it currently has none):

```yaml
        - task_key: gold_dbt
          depends_on:
            - task_key: medallion_ingest
          environment_key: dbt_env
          dbt_task:
            # ... unchanged ...
```

- [ ] **Step 5: Validate the bundle**

```bash
source infra/terraform/.env && databricks bundle validate
```
Expected: `Validation OK!` (the config resolves, the wheel artifact + the new task parse). If validation fails on the wheel dependency syntax, resolve per the note in Step 2 before committing.

- [ ] **Step 6: Commit**

```bash
git add databricks.yml
git commit -m "feat(bundle): medallion_ingest python_wheel_task (generate+bronze+silver) before gold

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01F1hdqzju3WbgEALYreWUjk"
```

---

### Task 4: live acceptance run + docs

Deploy and run the job for real (the acceptance bar), then document the closed follow-up. **This task needs live Databricks creds and spends one serverless run.**

**Files:**
- Modify: `docs/adr/0005-spark-execution-databricks-connect.md` (third Update)
- Modify: `README.md` (one line), `Makefile` (optional `bundle-run` note), `databricks.yml` (drop the stale "follow-up" comment)

- [ ] **Step 1: Deploy + run the job to SUCCESS**

```bash
source infra/terraform/.env
databricks bundle deploy
databricks bundle run vitals_medallion
```
Expected: the run reaches `TERMINATED SUCCESS` with all three tasks green (`medallion_ingest` → `gold_dbt` → `drift_monitor`).

**If `medallion_ingest` fails, resolve the flagged ambient-Spark risk from the run logs (decision tree):**
- `ModuleNotFoundError: databricks.connect` (the serverless runtime didn't provide it) → in `_spark()`'s ambient branch, switch to `from pyspark.sql import SparkSession; return SparkSession.builder.getOrCreate()` (pyspark is always present on Databricks compute), re-deploy, re-run.
- `getOrCreate()` errors without an active session → keep `databricks.connect` but add `databricks-connect` (matching the serverless client version) to `ingest_env.spec.dependencies`, re-deploy, re-run.
- Volume write/permission error on `/tmp/vitals_bronze` or the landing volume → confirm the job's service principal can write the volume; the `/tmp` path is local-writable on serverless, the volume is reached via `WorkspaceClient().files.upload` with ambient auth.
Make the minimal change, re-deploy, re-run until `TERMINATED SUCCESS`. Record the resolution in the ADR (Step 3).

- [ ] **Step 2: Spot-check the data the run produced**

```bash
source infra/terraform/.env
databricks api post /api/2.0/sql/statements --json '{"warehouse_id":"e2d0993979faf3d2","catalog":"vitals_silver","schema":"clinical","statement":"SELECT * FROM patient LIMIT 1"}' 2>/dev/null | python -c "import sys,json; d=json.load(sys.stdin); print('silver.patient columns:', [c['name'] for c in d['manifest']['schema']['columns']])"
```
Expected: the silver patient columns contain **no** HIPAA identifiers (no `name`/`address`/`birth_date`/`ssn`/etc.) — the PHI boundary held on-cluster. (If the `databricks api` form differs in this CLI version, equivalently check via Catalog Explorer or `databricks bundle run` task output; the assertion is: silver.patient is de-identified.)

- [ ] **Step 3: Document the closed follow-up in ADR 0005**

Append to `docs/adr/0005-spark-execution-databricks-connect.md`:

```markdown
## Update (2026-06-30) — the open follow-up is closed: the job runs the full medallion

Bronze + silver are now **in the job**: a single `python_wheel_task` (`medallion_ingest`, the wheel's
`vitals-medallion` entry point) runs **generate → bronze Delta → silver Delta** with the PHI +
non-empty gates, wired **before** `gold_dbt → drift_monitor`. One scheduled serverless run now does the
whole medallion, no laptop.

- **One codebase, three homes.** `vitals/env.py` resolves two signals at call time — `VITALS_BRONZE_DIR`
  (writable NDJSON dir; the job sets `/tmp/vitals_bronze`) and `VITALS_SPARK_MODE` (`ambient` on-cluster
  vs `serverless` for connect). Defaults reproduce the original behaviour, so the connect dev path and
  the DuckDB clone-and-run default are untouched.
- **Single task by design.** generate/bronze/silver share one Python process + Spark session and hand
  data off in-process, so they're one task; gold (dbt on a SQL warehouse) and drift (pandas, after gold)
  are different runtimes → separate tasks. Task boundaries follow the *runtime*, not the stage name.
- **In-job gates:** PHI (`assert_no_phi`) + non-empty counts hard-fail the task; the 26 dbt tests still
  gate gold. Cross-engine parity stays the dev/connect-time gate (the DuckDB baseline isn't on-cluster).
- **Free-Edition serverless wheel detail learned:** <fill in the exact resolution from the live run —
  the wheel-attachment syntax that worked, and the ambient-Spark accessor that worked>.
- Verified `TERMINATED SUCCESS` via `databricks bundle deploy && databricks bundle run vitals_medallion`.
```

Replace the `<fill in ...>` with the actual resolution from Step 1 (this is the one place the plan can't pre-write — it's the empirical result of the flagged risk).

- [ ] **Step 4: Update README + databricks.yml comment**

- In `databricks.yml`, replace the `gold_dbt` block comment that says "Upstream bronze/silver run today via databricks-connect ... promoting them into this job is a documented follow-up ..." with a one-liner noting the job now runs the full medallion (`medallion_ingest` → gold → drift).
- In `README.md`, the "Quickstart"/Databricks note: add one line that `databricks bundle run` executes the whole medallion on serverless (generate → bronze → silver → gold → drift).

- [ ] **Step 5: Commit**

```bash
git add docs/adr/0005-spark-execution-databricks-connect.md README.md databricks.yml Makefile
git commit -m "docs(adr): ADR 0005 follow-up closed — full medallion runs as a job (verified SUCCESS)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01F1hdqzju3WbgEALYreWUjk"
```

---

## Self-Review

**Spec coverage:**
- `env.py` (`bronze_dir`/`spark_mode`, call-time, defaults unchanged) → Task 1 Steps 1-4. ✓
- `generate.py` writes to `bronze_dir()` → Task 1 Steps 5-6. ✓
- `databricks_delta.py` honors `bronze_dir()` + `_spark()` mode branch → Task 1 Step 7. ✓
- `medallion_job.py` entry point + PHI + non-empty gates → Task 2 Steps 1-4. ✓
- pyproject `[build-system]` + `[project.scripts]` + version → Task 2 Step 5 (version already present). ✓
- Build artifacts gitignored → Task 2 Step 6. ✓
- `databricks.yml` wheel artifact + `medallion_ingest` + `ingest_env` + `gold_dbt depends_on` → Task 3. ✓
- Live deploy+run → SUCCESS (acceptance) → Task 4 Step 1. ✓
- PHI-free spot check → Task 4 Step 2. ✓
- ADR 0005 update + README/comment → Task 4 Steps 3-4. ✓
- Connect path + DuckDB default untouched (defaults) → Task 1 Step 8 (`make build` + suite green). ✓
- Flagged risk (wheel attachment + ambient Spark) → Task 3 Step 2 note + Task 4 Step 1 decision tree. ✓

**Placeholder scan:** The only intentionally-deferred content is the ADR's `<fill in the exact resolution from the live run>` — this is genuinely empirical (the resolution of the flagged platform risk) and Task 4 Step 3 instructs filling it from the run, not leaving it vague. No other TBDs; all code/config/commands are concrete.

**Type consistency:** `env.bronze_dir()`/`env.spark_mode()` defined in Task 1 are the exact names used in Task 1's wiring and Task 2's entry point. `medallion_job.main(argv)` / `_assert_nonempty(bronze, silver)` signatures match between Task 2's tests and implementation. The bundle's `package_name: vitals` + `entry_point: vitals-medallion` (Task 3) match the `[project.scripts]` name (Task 2 Step 5). `VITALS_BRONZE_DIR`/`VITALS_SPARK_MODE` strings are identical across `env.py`, `medallion_job.py`, and `databricks.yml`.
