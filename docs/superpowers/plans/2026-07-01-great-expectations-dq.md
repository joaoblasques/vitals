# Great Expectations Silver DQ Gate — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Great Expectations the gated data-quality contract for the silver layer — a code-defined expectation suite (coded-vocabulary value-sets + PHI boundary + ranges/uniqueness) that fails the build on any violation and runs in CI so it can't be skipped.

**Architecture:** A new `src/vitals/dq.py` — a pure, vocab-derived expectation spec separated from GE validation I/O — reads the silver tables (DuckDB → pandas) and validates them with GE 1.x (ephemeral context, per-table pandas batches). `make dq` runs it (exit non-zero on failure); CI installs the `dq` extra and runs `make dq` after `make build`. dbt keeps gating gold; GE gates silver.

**Tech Stack:** Great Expectations (GX Core) 1.x, pandas, DuckDB (upstream), pytest.

## Global Constraints

- **GE gates silver in CI (installed, not skipped)** — this is the deliberate difference from the Feast/pgvector demos. `make dq` exits non-zero on any violated expectation; CI runs `uv sync … --extra dq` + `make dq`. Clone-and-run (`make setup`/`make run`) does NOT need GE.
- **Coded-vocabulary value-sets come from `vitals.vocab`** (DRY): `valid_icd10()` = `set(vocab.ICD_DISPLAY) | set(vocab.TEXT_TO_ICD.values())` (= the 5 codes `M17.0, M25.561, M51.26, M54.5, M75.100`, which is exactly the distinct `silver.condition.icd10_code` set → strict set-membership is safe).
- **Real silver columns** (verified): `patient[patient_key, gender, age, _date_shift_days]`, `condition[patient_key, icd10_code, display, recovered_from_text]`, `observation[patient_key, obs_date, loinc_code, display, value_std, unit_std, metric]`, `pro[patient_key, survey_date, instrument, score]`, `wearable_daily[patient_key, day, steps, active_minutes, resting_hr, sleep_hours]`.
- **GE 1.x API** (verified via docs): `ctx = gx.get_context(mode="ephemeral")`; `ds = ctx.data_sources.add_pandas("pandas")`; `asset = ds.add_dataframe_asset(name=...)`; `bd = asset.add_batch_definition_whole_dataframe(name)`; `batch = bd.get_batch(batch_parameters={"dataframe": df})`; `res = batch.validate(gx.expectations.Expect...( ... ))`; `res.success` is a bool.
- **Complement, not replace** `dq_report.json` (untouched). Local DuckDB silver only (no GE-on-Databricks). No GE Data Docs HTML.
- Tests import `vitals` via pytest `pythonpath = ["src"]`; pure tests run with `--extra dev --extra local`; GE tests need `--extra dq`.

---

### Task 1: pure expectation spec (`dq.py` core) + hermetic tests

The Feast/GE-independent core: the vocab-derived value-sets and a plain-data description of every expectation, unit-testable without GE.

**Files:**
- Create: `src/vitals/dq.py`
- Test: `tests/test_dq.py`

**Interfaces:**
- Produces: `valid_icd10() -> list[str]`; `VALID_METRICS: list[str]`; `SILVER_PATIENT_COLUMNS: list[str]`; `PRO_MIN/PRO_MAX/STEPS_MIN/STEPS_MAX`; `expectations_spec() -> list[dict]` (each `{table, type, ...}`).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_dq.py`:

```python
from vitals import dq


def test_valid_icd10_is_the_vocab_set():
    # exactly the codes the pipeline standardizes to (ICD_DISPLAY keys ∪ TEXT_TO_ICD values)
    assert dq.valid_icd10() == sorted(["M17.0", "M25.561", "M51.26", "M54.5", "M75.100"])


def test_spec_covers_the_signature_expectations():
    spec = dq.expectations_spec()
    have = {(e["table"], e["type"], e.get("column")) for e in spec}
    # coded-vocab value-sets
    assert ("condition", "not_null", "icd10_code") in have
    assert ("condition", "in_set", "icd10_code") in have
    assert ("observation", "in_set", "metric") in have
    assert ("observation", "in_set", "unit_std") in have          # glucose == mg/dL (filtered)
    # PHI boundary + key + ranges
    assert ("patient", "columns_match_set", None) in have
    assert ("patient", "unique", "patient_key") in have
    assert ("pro", "between", "score") in have
    assert ("wearable_daily", "between", "steps") in have


def test_glucose_unit_expectation_is_filtered_to_glucose():
    spec = dq.expectations_spec()
    unit = next(e for e in spec if e["table"] == "observation" and e["column"] == "unit_std")
    assert unit["where"] == {"metric": "glucose"}
    assert unit["value_set"] == ["mg/dL"]
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --extra dev --extra local pytest tests/test_dq.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'vitals.dq'`.

- [ ] **Step 3: Create `src/vitals/dq.py` (pure core only)**

```python
"""Great Expectations DQ gate for the silver layer (ADR 0009). Pure spec (vocab-derived value-sets +
a plain-data description of every expectation) is separated from the GE validation I/O, so the suite is
testable without GE installed. GE GATES silver in CI (dbt gates gold); it is NOT an optional demo."""
from __future__ import annotations

import json
import sys
from pathlib import Path

from vitals import vocab

ROOT = Path(__file__).resolve().parents[2]
DB = ROOT / "data" / "vitals.duckdb"
OUT = ROOT / "data" / "ge_validation.json"

VALID_METRICS = ["adherence", "glucose", "heart_rate", "other", "pain"]
SILVER_PATIENT_COLUMNS = ["patient_key", "gender", "age", "_date_shift_days"]
PRO_MIN, PRO_MAX = 0, 100
STEPS_MIN, STEPS_MAX = 0, 50000


def valid_icd10() -> list[str]:
    """The coded-vocabulary contract: the ICD-10 codes silver standardizes to (the display map's keys +
    the text-recovery targets), from version-controlled vocab. Sorted for deterministic value-sets."""
    return sorted(set(vocab.ICD_DISPLAY) | set(vocab.TEXT_TO_ICD.values()))


def expectations_spec() -> list[dict]:
    """Plain-data description of the silver DQ contract — the GE layer materializes each into a real
    gx.expectations.* object. Kept pure so the suite's coverage is unit-testable without GE."""
    return [
        {"table": "condition", "type": "not_null", "column": "icd10_code"},
        {"table": "condition", "type": "in_set", "column": "icd10_code", "value_set": valid_icd10()},
        {"table": "observation", "type": "in_set", "column": "metric", "value_set": VALID_METRICS},
        {"table": "observation", "type": "in_set", "column": "unit_std",
         "value_set": ["mg/dL"], "where": {"metric": "glucose"}},
        {"table": "patient", "type": "columns_match_set", "column": None,
         "column_set": SILVER_PATIENT_COLUMNS},
        {"table": "patient", "type": "not_null", "column": "patient_key"},
        {"table": "patient", "type": "unique", "column": "patient_key"},
        {"table": "pro", "type": "between", "column": "score", "min": PRO_MIN, "max": PRO_MAX},
        {"table": "wearable_daily", "type": "between", "column": "steps",
         "min": STEPS_MIN, "max": STEPS_MAX},
    ]
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --extra dev --extra local pytest tests/test_dq.py -q`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/vitals/dq.py tests/test_dq.py
git commit -m "feat(dq): pure GE expectation spec for silver (vocab-derived value-sets)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01F1hdqzju3WbgEALYreWUjk"
```

---

### Task 2: GE validation gate (`dq.py` I/O) + `dq` extra + Makefile

The GE 1.x validation that turns the spec into a real gate, plus the packaging and make target — verified end-to-end with the extra installed.

**Files:**
- Modify: `src/vitals/dq.py` (append GE I/O + `main`)
- Modify: `pyproject.toml` (add `dq` extra)
- Modify: `Makefile` (`dq` target + `.PHONY`)
- Modify: `tests/test_dq.py` (append the GE "teeth" test)

**Interfaces:**
- Consumes: `expectations_spec()` (Task 1).
- Produces: `is_available() -> bool`; `validate(silver: dict | None = None) -> dict` (`{success, n_expectations, n_failed, results}`); `main()` (writes `OUT`, exits non-zero on failure).

- [ ] **Step 1: Append the GE I/O to `src/vitals/dq.py`**

```python
def is_available() -> bool:
    import importlib.util
    return importlib.util.find_spec("great_expectations") is not None


def _read_silver() -> dict:
    import duckdb
    con = duckdb.connect(str(DB))
    tables = {t: con.execute(f"SELECT * FROM silver.{t}").df()
              for t in ("condition", "observation", "patient", "pro", "wearable_daily")}
    con.close()
    return tables


def _expectation(item: dict):
    import great_expectations as gx
    t, col = item["type"], item.get("column")
    if t == "not_null":
        return gx.expectations.ExpectColumnValuesToNotBeNull(column=col)
    if t == "in_set":
        return gx.expectations.ExpectColumnValuesToBeInSet(column=col, value_set=item["value_set"])
    if t == "unique":
        return gx.expectations.ExpectColumnValuesToBeUnique(column=col)
    if t == "between":
        return gx.expectations.ExpectColumnValuesToBeBetween(
            column=col, min_value=item["min"], max_value=item["max"])
    if t == "columns_match_set":
        return gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=item["column_set"], exact_match=True)
    raise ValueError(f"unknown expectation type {t!r}")


def validate(silver: dict | None = None) -> dict:
    """Run the silver expectation suite with GE (ephemeral context, one pandas batch per expectation —
    filtered where a `where` clause is given, e.g. glucose-only unit check). Returns an aggregate result.
    Pass `silver` (dict of table->DataFrame) to validate injected data; default reads the built silver."""
    import great_expectations as gx
    if silver is None:
        silver = _read_silver()
    ctx = gx.get_context(mode="ephemeral")
    ds = ctx.data_sources.add_pandas("pandas")
    results = []
    for i, item in enumerate(expectations_spec()):
        df = silver[item["table"]]
        for k, v in item.get("where", {}).items():
            df = df[df[k] == v]
        asset = ds.add_dataframe_asset(name=f"a{i}_{item['table']}")
        bd = asset.add_batch_definition_whole_dataframe(f"b{i}")
        batch = bd.get_batch(batch_parameters={"dataframe": df})
        res = batch.validate(_expectation(item))
        results.append({
            "table": item["table"], "type": item["type"],
            "column": item.get("column") or "columns",
            "success": bool(res.success),
        })
    n_failed = sum(1 for r in results if not r["success"])
    return {"success": n_failed == 0, "n_expectations": len(results),
            "n_failed": n_failed, "results": results}


def main() -> None:
    result = validate()
    OUT.write_text(json.dumps(result, indent=2))
    passed = result["n_expectations"] - result["n_failed"]
    print(f"GE silver DQ: {passed}/{result['n_expectations']} expectations passed")
    for r in result["results"]:
        if not r["success"]:
            print(f"  FAILED: {r['table']}.{r['column']} [{r['type']}]")
    sys.exit(0 if result["success"] else 1)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Add the `dq` optional extra to `pyproject.toml`**

After the `metrics` extra line, add:
```toml
# Data-quality gate — Great Expectations validates the silver contract in CI (ADR 0009). A GATE (run in
# CI), not an optional demo — but kept out of core so clone-and-run/`make run` stays lean.
dq = ["great-expectations>=1.0"]
```

- [ ] **Step 3: Add the `make dq` target**

In `Makefile`, add `dq` to `.PHONY` and add (after the `metrics-query` block):
```make
dq:             ## Great Expectations gate: validate the silver DQ contract (needs `uv sync --extra dq` + `make build`)
	PYTHONPATH=src ./.venv/bin/python -m vitals.dq
```

- [ ] **Step 4: Append the GE "teeth" test to `tests/test_dq.py`**

First add these imports to the TOP of the file (with the existing `from vitals import dq`), so nothing
is imported inside a function body (ruff-clean):
```python
import importlib.util

import pandas as pd
import pytest
```
Then append the test:
```python
@pytest.mark.skipif(
    importlib.util.find_spec("great_expectations") is None,
    reason="needs the `dq` extra (great-expectations); runs in CI where it's installed",
)
def test_validate_catches_a_bad_silver_batch():
    """The gate must have teeth: an out-of-vocab icd10_code + a PHI column must fail validation."""
    good = {
        "condition": pd.DataFrame({"patient_key": ["p1"], "icd10_code": ["M54.5"]}),
        "observation": pd.DataFrame({"patient_key": ["p1"], "metric": ["pain"], "unit_std": ["score"]}),
        "patient": pd.DataFrame({"patient_key": ["p1"], "gender": ["m"], "age": [70], "_date_shift_days": [3]}),
        "pro": pd.DataFrame({"patient_key": ["p1"], "score": [40]}),
        "wearable_daily": pd.DataFrame({"patient_key": ["p1"], "steps": [8000]}),
    }
    assert dq.validate(good)["success"] is True

    bad = {k: v.copy() for k, v in good.items()}
    bad["condition"]["icd10_code"] = ["Z99.9"]            # not in the valid ICD-10 set
    bad["patient"]["name"] = ["Jane Doe"]                 # a PHI column snuck in
    result = dq.validate(bad)
    assert result["success"] is False
    failed = {(r["table"], r["type"]) for r in result["results"] if not r["success"]}
    assert ("condition", "in_set") in failed
    assert ("patient", "columns_match_set") in failed
```

- [ ] **Step 5: Install the extra and run the gate end-to-end**

```bash
uv sync --extra dev --extra local --extra dq
make build                 # writes the conformed silver into data/vitals.duckdb
make dq                    # the gate — must exit 0 on the real silver
echo "exit: $?"
```
Expected: `make dq` prints `GE silver DQ: 9/9 expectations passed` and exits 0; `data/ge_validation.json` shows `"success": true`. If any expectation fails on the real silver, STOP and report which one (it means either a real DQ issue or a spec/column mismatch) — do not relax the expectation to force green.

- [ ] **Step 6: Run the teeth test + confirm the pure tests still pass**

```bash
uv run --extra dev --extra local --extra dq pytest tests/test_dq.py -q      # pure + teeth
uv run --extra dev --extra local pytest tests/test_dq.py -q                 # pure pass, teeth SKIPS
uv run ruff check .
```
Expected: first — all pass (3 pure + 1 teeth); second — 3 pass, 1 skipped; ruff clean.

- [ ] **Step 7: Commit**

```bash
git add src/vitals/dq.py pyproject.toml Makefile tests/test_dq.py uv.lock
git commit -m "feat(dq): GE silver validation gate (exit non-zero on violation) + dq extra + make dq

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01F1hdqzju3WbgEALYreWUjk"
```

---

### Task 3: wire the gate into CI + ADR 0009 + README

Make the gate run on every push (the "can't be skipped" half) and document it.

**Files:**
- Modify: `.github/workflows/ci.yml` (install `--extra dq`; add a `make dq` step)
- Create: `docs/adr/0009-great-expectations-silver-dq.md`
- Modify: `README.md`

- [ ] **Step 1: Install the `dq` extra + add the gate step in CI**

In `.github/workflows/ci.yml`, change the install step:
```yaml
      - name: Install deps (from lock)
        run: uv sync --extra dev --extra local --extra dq
```
and add a step immediately AFTER the existing `DQ gate — local pipeline` (`make build`) step:
```yaml
      - name: Great Expectations gate (silver DQ contract)
        run: make dq
```
(`make build` in the prior step writes `data/vitals.duckdb`; `make dq` validates that silver. A failed expectation fails CI.)

- [ ] **Step 2: Write ADR 0009**

Create `docs/adr/0009-great-expectations-silver-dq.md`:

```markdown
# ADR 0009 — Great Expectations as the gated silver DQ contract

**Status:** accepted · 2026-07-01

## Context
The project promises "coded-vocabulary data quality — validate vocabularies as DQ contracts (Great
Expectations), not vibes" and "DQ gates before exposing data." But silver DQ was a *descriptive* report
(`dq_report.json` — metrics, only the PHI check actually failed the build), and dbt tests gated only
**gold**. Great Expectations was named in the stack but unused.

## Decision
Make GE the **gating** DQ contract for **silver** (the consumption layer / PHI boundary). A code-defined
suite (`src/vitals/dq.py`) validates the conformed silver tables with GX Core 1.x and **exits non-zero on
any violation**; `make dq` runs it and **CI runs it after `make build`**, so it can't be skipped.

Signature expectations (the health-data DQ that sets this apart):
- **Coded-vocabulary value-sets:** every `condition.icd10_code` is not-null and ∈ the valid ICD-10 set
  (from `vitals.vocab`); `observation.metric` ∈ the standard set; glucose `unit_std` == `mg/dL`
  (unit standardization held). *Validated, not vibed.*
- **PHI boundary:** `silver.patient` columns match the allowed set exactly (no identifier can sneak in).
- **Ranges + key:** PRO score 0–100, steps 0–50000, `patient_key` unique + not-null.

Key choices:
- **A gate runs, it doesn't skip.** Unlike the Feast/pgvector *demos* (optional extras that skip in CI),
  GE is a gate — CI installs the `dq` extra and runs it. Clone-and-run stays lean (`make run` needs no GE).
- **Value-sets from version-controlled `vocab`** (DRY) — the same standards silver conforms to.
- **Complements, not replaces** the descriptive `dq_report.json`. dbt still gates gold; GE gates silver.

## Consequences
- New `dq` dependency group (`great-expectations>=1.0`); a `make dq` gate; `data/ge_validation.json`.
- CI has a new hard gate on silver — a violated expectation fails the build.

## Alternatives considered
- **Keep the descriptive report:** metrics you read after the fact don't *stop* bad data reaching gold.
- **dbt tests only:** they gate gold, not the silver conform + the coded-vocabulary contract at the PHI
  boundary; GE's value-set expectations express "valid vocabulary" first-class.
- **GE Data Docs site / GE-on-Databricks:** out of scope (a lean gate + JSON result; local DuckDB silver).
```

- [ ] **Step 3: Update the README stack line**

In `README.md` tech stack, `**Great Expectations**` is currently just listed. Add a parenthetical so it's true:
```markdown
**Great Expectations** (gates the silver coded-vocabulary DQ contract in CI — `make dq`) ·
```
(Keep the surrounding stack list intact; just make the GE item reflect that it's wired + gating.)

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml docs/adr/0009-great-expectations-silver-dq.md README.md
git commit -m "ci+docs(dq): gate silver with Great Expectations in CI + ADR 0009 + README

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01F1hdqzju3WbgEALYreWUjk"
```

---

## Self-Review

**Spec coverage:**
- Pure vocab-derived spec (`valid_icd10`, `expectations_spec`) → Task 1. ✓
- GE validation gate (`validate`/`main`, exit non-zero) → Task 2 Step 1. ✓
- Coded-vocab value-sets (icd10 set + 100% coded; metric set; glucose mg/dL) → Task 1 spec + Task 2 GE. ✓
- PHI boundary (columns_match_set) + ranges + uniqueness → Task 1 spec + Task 2 GE. ✓
- `dq` extra + `make dq` → Task 2 Steps 2-3. ✓
- Teeth test (catches violations) + pure tests always run → Task 2 Steps 4,6. ✓
- CI gate (install `--extra dq` + `make dq` step) → Task 3 Step 1. ✓
- ADR 0009 + README made-true → Task 3 Steps 2-3. ✓
- Complements dq_report.json (untouched); local only → no task modifies `lakehouse` DQ; no Databricks. ✓

**Placeholder scan:** none. The `__import__(... find_spec ...)` in the teeth-test skipif mirrors the existing feature-store test idiom (kept for consistency; a top-level `import importlib.util` is equally fine if the implementer prefers). All code/commands concrete.

**Type consistency:** `valid_icd10()`/`expectations_spec()` (Task 1) are consumed by `validate`/`_expectation` (Task 2) with matching shapes (`item["type"]`, `item["value_set"]`, `item["where"]`, `item["column_set"]`). `validate(silver=None)` return keys (`success`/`results`/`table`/`type`) match the teeth-test assertions (Task 2 Step 4) and `main()` (Task 2 Step 1). The `dq` extra name matches the CI `--extra dq` (Task 3) and the Makefile note.
```
