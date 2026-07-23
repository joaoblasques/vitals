# Unified Safe Harbor PHI-column gate — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the two drifted, 5-category PHI blocklists (DuckDB `lakehouse.py` vs. Databricks
`databricks_delta.py`) with one shared module covering all 18 HIPAA Safe Harbor identifier
categories, applied to every silver table (not just `patient`) on both engines, and prove the
Databricks-path check has teeth in CI.

**Architecture:** A new pure module `src/vitals/phi.py` defines `SAFE_HARBOR_PATTERNS` (18
categories → column-name pattern sets) and `assert_no_phi(table, columns)`. Both engines call it
per-table instead of maintaining their own blocklists. A new pytest file proves 18-category
coverage, per-category poisoning, zero false positives on the real 8 silver tables, and that the
Databricks call site is wired correctly — all without a live cluster (the Databricks module's I/O
functions are lazy-imported, so `assert_no_phi` and column-introspection are already callable
hermetically).

**Tech Stack:** Python 3, pytest, DuckDB (`duckdb`), no new dependencies.

## Global Constraints

- No new external dependency — `SAFE_HARBOR_PATTERNS` is a plain `dict[str, set[str]]` (per spec
  Non-goals).
- Column-name/pattern matching only — no content-level scanning of free-text fields (per spec Scope
  decisions). Do not add any regex-over-values logic.
- Every silver table on both engines must be checked: `patient`, `target`, `observation`,
  `condition`, `note`, `claim`, `pro`, `wearable_daily` (8 tables, per spec).
- The existing 8 real silver tables' current columns must produce **zero** false positives — this is
  a hard regression guard, not a nice-to-have.
- `git commit -m` messages: use the project's existing style (`type(scope): summary`), matching
  recent commits like `fix(security): salt patient_key across all engines (ADR 0002 gap)`.
- ADR belongs in `docs/adr/0011-unified-phi-column-gate.md` (next sequential number after 0010).

---

## Task 1: `src/vitals/phi.py` — canonical Safe Harbor pattern set + `assert_no_phi()`

**Files:**
- Create: `src/vitals/phi.py`
- Test: `tests/test_phi.py`

**Interfaces:**
- Produces: `SAFE_HARBOR_PATTERNS: dict[str, set[str]]` (18 keys, one per Safe Harbor category,
  values are lowercase column-name substrings that indicate that category).
- Produces: `assert_no_phi(table: str, columns: set[str]) -> None` — raises
  `AssertionError(f"PHI leaked into silver.{table}: column '{col}' matches Safe Harbor category
  '{category}'")` on the **first** match found (deterministic order: iterate `SAFE_HARBOR_PATTERNS`
  in insertion order, then iterate `columns` in sorted order, so the same input always raises the
  same message). Matching is case-insensitive substring match: a column matches a category if
  `pattern in column.lower()` for any pattern in that category's set.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_phi.py`:

```python
import pytest

from vitals import phi

# The 18 HIPAA Safe Harbor identifier categories (45 CFR 164.514(b)(2)).
EXPECTED_CATEGORIES = {
    "names",
    "geographic_subdivisions",
    "dates",
    "telephone_numbers",
    "fax_numbers",
    "email_addresses",
    "ssn",
    "medical_record_numbers",
    "health_plan_beneficiary_numbers",
    "account_numbers",
    "certificate_license_numbers",
    "vehicle_identifiers",
    "device_identifiers",
    "urls",
    "ip_addresses",
    "biometric_identifiers",
    "full_face_photos",
    "other_unique_identifiers",
}


def test_covers_all_18_safe_harbor_categories():
    assert set(phi.SAFE_HARBOR_PATTERNS) == EXPECTED_CATEGORIES


def test_every_category_has_at_least_one_pattern():
    for category, patterns in phi.SAFE_HARBOR_PATTERNS.items():
        assert patterns, f"category {category!r} has no patterns"


@pytest.mark.parametrize(
    "category,poisoned_column",
    [
        ("names", "patient_name"),
        ("geographic_subdivisions", "street_address"),
        ("dates", "birthDate"),
        ("telephone_numbers", "phone_number"),
        ("fax_numbers", "fax_number"),
        ("email_addresses", "email"),
        ("ssn", "ssn"),
        ("medical_record_numbers", "medical_record_number"),
        ("health_plan_beneficiary_numbers", "health_plan_beneficiary_id"),
        ("account_numbers", "account_number"),
        ("certificate_license_numbers", "license_number"),
        ("vehicle_identifiers", "vehicle_vin"),
        ("device_identifiers", "device_serial"),
        ("urls", "url"),
        ("ip_addresses", "ip_address"),
        ("biometric_identifiers", "fingerprint_id"),
        ("full_face_photos", "photo_url_face"),
        ("other_unique_identifiers", "identifier"),
    ],
)
def test_assert_no_phi_catches_each_category(category, poisoned_column):
    with pytest.raises(AssertionError, match=category):
        phi.assert_no_phi("some_table", {"patient_key", poisoned_column})


def test_assert_no_phi_passes_clean_columns():
    phi.assert_no_phi("patient", {"patient_key", "gender", "age", "_date_shift_days"})


def test_assert_no_phi_error_names_the_table_and_column():
    with pytest.raises(AssertionError) as exc_info:
        phi.assert_no_phi("note", {"patient_key", "email"})
    msg = str(exc_info.value)
    assert "note" in msg
    assert "email" in msg
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_phi.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'vitals.phi'` (or similar import error) —
the module doesn't exist yet.

- [ ] **Step 3: Write `src/vitals/phi.py`**

```python
"""Canonical HIPAA Safe Harbor identifier check (45 CFR 164.514(b)(2)) — the single source of
truth for "does this column look like PHI", shared by every silver-writing engine (DuckDB,
Databricks) so the two can never drift again (ADR 0011).

Column-name/pattern matching only — this does NOT scan cell values for embedded identifiers (e.g.
an SSN inside a free-text note). That is a documented, deliberate gap; see ADR 0011.
"""
from __future__ import annotations

# 18 Safe Harbor categories -> lowercase substrings that indicate that category in a column name.
SAFE_HARBOR_PATTERNS: dict[str, set[str]] = {
    "names": {"name", "patient_name", "first_name", "last_name"},
    "geographic_subdivisions": {"address", "street", "city", "zip", "postal"},
    "dates": {"birthdate", "birth_date", "dob", "admit_date", "discharge_date", "death_date"},
    "telephone_numbers": {"phone", "telephone", "mobile_number"},
    "fax_numbers": {"fax"},
    "email_addresses": {"email"},
    "ssn": {"ssn", "social_security"},
    "medical_record_numbers": {"medical_record_number", "mrn"},
    "health_plan_beneficiary_numbers": {"health_plan_beneficiary", "beneficiary_id", "beneficiary_number"},
    "account_numbers": {"account_number", "account_no"},
    "certificate_license_numbers": {"license_number", "certificate_number"},
    "vehicle_identifiers": {"vehicle_vin", "vin", "license_plate"},
    "device_identifiers": {"device_serial", "device_id", "serial_number"},
    "urls": {"url", "website"},
    "ip_addresses": {"ip_address", "ip_addr"},
    "biometric_identifiers": {"fingerprint", "biometric", "retina", "voiceprint"},
    "full_face_photos": {"photo", "headshot", "face_image"},
    "other_unique_identifiers": {"identifier", "unique_id"},
}


def assert_no_phi(table: str, columns: set[str]) -> None:
    """Raise if any column in `columns` matches a Safe Harbor identifier pattern.

    Deterministic: categories checked in SAFE_HARBOR_PATTERNS order, columns in sorted order, so the
    same offending input always produces the same error message (the first match).
    """
    lowered = {c: c.lower() for c in columns}
    for category, patterns in SAFE_HARBOR_PATTERNS.items():
        for col in sorted(columns):
            col_lower = lowered[col]
            if any(pattern in col_lower for pattern in patterns):
                raise AssertionError(
                    f"PHI leaked into silver.{table}: column '{col}' matches "
                    f"Safe Harbor category '{category}'"
                )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_phi.py -v`
Expected: PASS (21 tests: 2 structural + 18 parametrized category + 1 clean-pass).

- [ ] **Step 5: Commit**

```bash
git add src/vitals/phi.py tests/test_phi.py
git commit -m "feat(security): add unified 18-category Safe Harbor PHI check (vitals.phi)"
```

---

## Task 2: Regression-guard the real 8 silver tables against the new check

**Files:**
- Modify: `tests/test_phi.py`

**Interfaces:**
- Consumes: `phi.assert_no_phi(table: str, columns: set[str]) -> None` from Task 1.
- Consumes (read-only, for the real column lists): the column sets below, copied from the current
  `CREATE OR REPLACE TABLE silver.*` statements in `src/vitals/lakehouse.py` — this test hard-codes
  them rather than importing `lakehouse` (which requires a live DuckDB build) so it stays hermetic
  and fast.

This task exists as its own step (not folded into Task 1) because it is the regression guard that
justifies broadening the check — it must prove the *real* schemas still pass before Task 3/4 wire
the new module into the engines, so a false positive is caught here first, not after rollout.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_phi.py`:

```python
# The real current column sets for every silver table (src/vitals/lakehouse.py /
# src/vitals/backends/databricks_delta.py _silver_statements()). Hard-coded here (not imported)
# so this test stays hermetic — no DuckDB build or Spark session required.
REAL_SILVER_COLUMNS = {
    "patient": {"patient_key", "gender", "age", "_date_shift_days"},
    "target": {"patient_key", "surgery_90d"},
    "observation": {"patient_key", "obs_date", "loinc_code", "display", "value_std", "unit_std", "metric"},
    "condition": {"patient_key", "icd10_code", "display", "recovered_from_text"},
    "note": {"patient_key", "note_date", "text"},
    "claim": {"patient_key", "claim_date", "procedure_code", "procedure_display", "dx_code", "billed", "paid", "status", "denied"},
    "pro": {"patient_key", "survey_date", "instrument", "score"},
    "wearable_daily": {"patient_key", "day", "steps", "active_minutes", "resting_hr", "sleep_hours"},
}


@pytest.mark.parametrize("table,columns", sorted(REAL_SILVER_COLUMNS.items()))
def test_assert_no_phi_passes_every_real_silver_table(table, columns):
    """Regression guard: broadening to 18 categories must not false-positive on real schemas."""
    phi.assert_no_phi(table, columns)
```

- [ ] **Step 2: Run test to verify it fails (or passes) and inspect why**

Run: `uv run pytest tests/test_phi.py -k test_assert_no_phi_passes_every_real_silver_table -v`
Expected: PASS for all 8 tables, since `phi.py` was hand-written against exactly this table list in
Task 1. If any table fails here, it means a category pattern is too broad (e.g. `"note"` column in
the `note` table triggering a false positive) — fix the offending pattern in `SAFE_HARBOR_PATTERNS`
(Task 1's file) before proceeding, and re-run.

- [ ] **Step 3: (No implementation step — this task is the test itself.)**

- [ ] **Step 4: Run the full test file to confirm nothing else broke**

Run: `uv run pytest tests/test_phi.py -v`
Expected: PASS, all tests (Task 1's 21 + this task's 8 = 29 tests).

- [ ] **Step 5: Commit**

```bash
git add tests/test_phi.py
git commit -m "test(security): guard the 8 real silver schemas against false PHI positives"
```

---

## Task 3: Wire `phi.assert_no_phi` into the DuckDB engine (`lakehouse.py`), all 8 tables

**Files:**
- Modify: `src/vitals/lakehouse.py:171-173`
- Test: `tests/test_lakehouse.py` (create if it doesn't exist — check first)

**Interfaces:**
- Consumes: `phi.assert_no_phi(table: str, columns: set[str]) -> None` from Task 1.
- Produces: `build()` in `lakehouse.py` now checks all 8 silver tables (previously only `patient`)
  before writing `SILVER_BASELINE`. No change to `build()`'s return type (`dict`) or the
  `dq_report.json` shape.

- [ ] **Step 1: Check for an existing lakehouse test file**

Run: `ls tests/ | grep lakehouse`
If `tests/test_lakehouse.py` exists, read it first to match its existing style/fixtures before
adding to it in Step 1 below. If it doesn't exist, Step 1 creates it fresh (below assumes it does
not exist; adjust import/fixture setup to match if it does).

- [ ] **Step 2: Write the failing test**

Create `tests/test_lakehouse.py` (or add to it if it exists):

```python
import duckdb

from vitals import lakehouse, phi


def test_build_checks_phi_on_every_silver_table(monkeypatch, tmp_path):
    """build() must call phi.assert_no_phi for all 8 silver tables, not just patient."""
    checked_tables = []
    original = phi.assert_no_phi

    def spy(table, columns):
        checked_tables.append(table)
        return original(table, columns)

    monkeypatch.setattr(lakehouse.phi, "assert_no_phi", spy)
    monkeypatch.setattr(lakehouse, "DB", tmp_path / "vitals.duckdb")
    monkeypatch.setattr(lakehouse, "DQ_OUT", tmp_path / "dq_report.json")
    monkeypatch.setattr(lakehouse, "SILVER_BASELINE", tmp_path / "silver_baseline.json")

    lakehouse.build()

    assert set(checked_tables) == {
        "patient", "target", "observation", "condition",
        "note", "claim", "pro", "wearable_daily",
    }
```

Note: this test runs the real `build()`, which reads NDJSON from `data/bronze/` — it requires the
repo's existing bronze fixture data to be present (same precondition as `make build` today). If
`data/bronze/*.ndjson` files don't exist in this checkout, run `make generate` first (check
`Makefile` for the exact target name) before running this test.

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/test_lakehouse.py -v`
Expected: FAIL — `checked_tables` only contains `"patient"` (or the assertion errors because
`lakehouse` doesn't import `phi` yet / `AttributeError: module has no attribute 'phi'`).

- [ ] **Step 4: Modify `src/vitals/lakehouse.py`**

Add the import near the top (after the existing `from vitals import salt` line, line 16):

```python
from vitals import phi, salt
```

Replace the single PHI assertion block (lines 171-173):

```python
    # de-id assertion: silver.patient must carry no PHI columns
    cols = {c[0] for c in con.execute("DESCRIBE silver.patient").fetchall()}
    assert not (cols & {"name", "identifier", "address", "birthDate"}), f"PHI leaked into silver: {cols}"
```

with a loop over all 8 silver tables, placed **before** the `SILVER_BASELINE` write (which already
introspects `information_schema.tables` for the table list — reuse that same list):

```python
    # de-id assertion: no silver table may carry a Safe Harbor PHI-pattern column (ADR 0002, ADR 0011)
    silver_tables = [r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='silver' ORDER BY 1"
    ).fetchall()]
    for table in silver_tables:
        cols = {c[0] for c in con.execute(f"DESCRIBE silver.{table}").fetchall()}
        phi.assert_no_phi(table, cols)
```

This introduces a second `information_schema.tables` query ahead of the existing one at line
177-179 (`tables = [...]`). Since both fetch the identical list, replace the later block to reuse
`silver_tables` instead of re-querying:

```python
    # Per-table silver row counts -> baseline file, so other backends (Delta-on-UC) can verify
    # row-count parity without needing DuckDB. Regenerated every build, so it tracks the data.
    baseline = {t: _scalar(con, f"SELECT count(*) FROM silver.{t}") for t in silver_tables}
    SILVER_BASELINE.write_text(json.dumps(baseline, indent=2))
```

(This removes the old duplicate `tables = [r[0] for r in con.execute(...)]` line entirely — one
query now serves both the PHI loop and the baseline write.)

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/test_lakehouse.py -v`
Expected: PASS.

- [ ] **Step 6: Run the full existing test suite + a real build to confirm no regression**

Run: `uv run pytest -q`
Expected: PASS, all tests (no existing test should break — `build()`'s external behavior,
`dq_report.json` shape, and `SILVER_BASELINE` content are unchanged).

Run: `make build`
Expected: exits 0, same as before this change. If it raises `AssertionError: PHI leaked into
silver.<table>`, a real silver column is false-positiving against a Safe Harbor pattern — this
means Task 2's regression guard had a gap (its hard-coded column list drifted from the live schema);
fix the pattern in `phi.py` and re-add a test case to `tests/test_phi.py` for the missed column
before proceeding.

- [ ] **Step 7: Commit**

```bash
git add src/vitals/lakehouse.py tests/test_lakehouse.py
git commit -m "feat(security): check all 8 silver tables for PHI in the DuckDB engine (ADR 0011)"
```

---

## Task 4: Wire `phi.assert_no_phi` into the Databricks engine, all 8 tables

**Files:**
- Modify: `src/vitals/backends/databricks_delta.py:44-45, 231-250, 311-320`
- Modify: `src/vitals/medallion_job.py:30`
- Test: `tests/test_databricks_delta.py` (check if it exists first)

**Interfaces:**
- Consumes: `phi.assert_no_phi(table: str, columns: set[str]) -> None` from Task 1.
- Produces: `silver_table_columns(table: str) -> list[str]` — replaces `silver_patient_columns()`
  (generalized to any table; callers needing just `patient` now call
  `silver_table_columns("patient")`).
- Produces: `assert_all_silver_no_phi(spark) -> None` — new function that, for every table in
  `SILVER_TABLES`, fetches its columns via `spark.table(...).columns` and calls
  `phi.assert_no_phi(table, set(columns))`. Replaces the old `assert_no_phi(columns: list[str])`
  (which only checked `patient` against the local `PHI_COLUMNS` set — that local set and function
  are deleted).
- Breaking change (internal, no other module calls these): `medallion_job.py:30`'s call site
  changes from `dx.assert_no_phi(dx.silver_patient_columns())` to `dx.assert_all_silver_no_phi(dx._spark())`.
  `databricks_delta.py:311-320`'s `main_silver()` changes similarly.

- [ ] **Step 1: Check for an existing databricks_delta test file**

Run: `ls tests/ | grep databricks`
Read it first if present, to match existing fixture/mocking style (the module lazy-imports
`databricks.connect`/`databricks.sdk`, so hermetic tests mock `_spark()` rather than requiring a
workspace — check how existing tests in this file, if any, already do that).

- [ ] **Step 2: Write the failing test**

Create `tests/test_databricks_delta.py` (or add to it if it exists):

```python
import pytest

from vitals.backends import databricks_delta as dx


class _FakeTable:
    def __init__(self, columns):
        self.columns = columns


class _FakeSpark:
    """Minimal stand-in for a SparkSession — only implements .table(name).columns."""

    def __init__(self, tables: dict[str, list[str]]):
        self._tables = tables

    def table(self, full_name: str):
        table_name = full_name.rsplit(".", 1)[-1]
        return _FakeTable(self._tables[table_name])


REAL_SILVER_COLUMNS = {
    "patient": ["patient_key", "gender", "age", "_date_shift_days"],
    "target": ["patient_key", "surgery_90d"],
    "observation": ["patient_key", "obs_date", "loinc_code", "display", "value_std", "unit_std", "metric"],
    "condition": ["patient_key", "icd10_code", "display", "recovered_from_text"],
    "note": ["patient_key", "note_date", "text"],
    "claim": ["patient_key", "claim_date", "procedure_code", "procedure_display", "dx_code", "billed", "paid", "status", "denied"],
    "pro": ["patient_key", "survey_date", "instrument", "score"],
    "wearable_daily": ["patient_key", "day", "steps", "active_minutes", "resting_hr", "sleep_hours"],
}


def test_assert_all_silver_no_phi_passes_real_schemas():
    fake_spark = _FakeSpark(REAL_SILVER_COLUMNS)
    dx.assert_all_silver_no_phi(fake_spark)  # must not raise


def test_assert_all_silver_no_phi_checks_every_table():
    checked = []
    tables = dict(REAL_SILVER_COLUMNS)
    fake_spark = _FakeSpark(tables)

    class _SpyTable(_FakeTable):
        pass

    orig_table = fake_spark.table

    def spy_table(full_name):
        checked.append(full_name.rsplit(".", 1)[-1])
        return orig_table(full_name)

    fake_spark.table = spy_table
    dx.assert_all_silver_no_phi(fake_spark)
    assert set(checked) == set(dx.SILVER_TABLES)


def test_assert_all_silver_no_phi_catches_a_leak_without_a_cluster():
    """The Databricks-path check must have teeth, provable with zero live infra (CI's proof point)."""
    poisoned = dict(REAL_SILVER_COLUMNS)
    poisoned["note"] = poisoned["note"] + ["email"]
    fake_spark = _FakeSpark(poisoned)
    with pytest.raises(AssertionError, match="email_addresses"):
        dx.assert_all_silver_no_phi(fake_spark)


def test_silver_table_columns_reads_any_table():
    fake_spark = _FakeSpark(REAL_SILVER_COLUMNS)
    assert dx.silver_table_columns(fake_spark, "condition") == REAL_SILVER_COLUMNS["condition"]
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/test_databricks_delta.py -v`
Expected: FAIL — `AttributeError: module 'vitals.backends.databricks_delta' has no attribute
'assert_all_silver_no_phi'` (and `silver_table_columns`).

- [ ] **Step 4: Modify `src/vitals/backends/databricks_delta.py`**

Add the import (near the top, after `from vitals import env, salt` at line 21):

```python
from vitals import env, phi, salt
```

Delete the local blocklist (line 44-45):

```python
# Columns that must NEVER appear in silver.patient (HIPAA Safe Harbor identifiers).
PHI_COLUMNS = {"name", "identifier", "address", "birthDate", "ssn"}
```

Replace `silver_patient_columns()` and `assert_no_phi()` (lines 242-250) with:

```python
def silver_table_columns(spark, table: str) -> list[str]:
    return spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.{table}").columns


def assert_all_silver_no_phi(spark) -> None:
    """The project's signature check, generalized: no HIPAA identifier survives into ANY silver
    table (ADR 0002, ADR 0011). Uses the shared vitals.phi module so this engine's check can never
    drift from the DuckDB engine's again."""
    for table in SILVER_TABLES:
        cols = set(silver_table_columns(spark, table))
        phi.assert_no_phi(table, cols)
```

Update `main_silver()` (lines 311-320) — replace:

```python
def main_silver() -> None:
    print(f"[silver->delta] building {len(SILVER_TABLES)} tables in {SILVER_CATALOG}.{SILVER_SCHEMA} ...")
    remote = build_silver()
    cols = silver_patient_columns()
    assert_no_phi(cols)  # PHI boundary — fail hard before reporting parity
    print(f"  PHI boundary OK — silver.patient columns: {cols}")
    ok = _print_parity("table", parity_report(silver_baseline(), remote))
    print(f"\n{'✅ silver parity: all tables match local DuckDB' if ok else '❌ silver parity FAILED'}")
    if not ok:
        raise SystemExit(1)
```

with:

```python
def main_silver() -> None:
    print(f"[silver->delta] building {len(SILVER_TABLES)} tables in {SILVER_CATALOG}.{SILVER_SCHEMA} ...")
    remote = build_silver()
    assert_all_silver_no_phi(_spark())  # PHI boundary — fail hard before reporting parity, all 8 tables
    print(f"  PHI boundary OK — all {len(SILVER_TABLES)} silver tables checked")
    ok = _print_parity("table", parity_report(silver_baseline(), remote))
    print(f"\n{'✅ silver parity: all tables match local DuckDB' if ok else '❌ silver parity FAILED'}")
    if not ok:
        raise SystemExit(1)
```

- [ ] **Step 5: Modify `src/vitals/medallion_job.py:30`**

Replace:

```python
    dx.assert_no_phi(dx.silver_patient_columns())   # PHI boundary — hard gate
```

with:

```python
    dx.assert_all_silver_no_phi(dx._spark())        # PHI boundary — hard gate, all 8 silver tables
```

- [ ] **Step 6: Run test to verify it passes**

Run: `uv run pytest tests/test_databricks_delta.py -v`
Expected: PASS.

- [ ] **Step 7: Run the full test suite to confirm no regression**

Run: `uv run pytest -q`
Expected: PASS. No other module calls `assert_no_phi`, `silver_patient_columns`, or `PHI_COLUMNS`
from `databricks_delta.py` (verify with `grep -rn "silver_patient_columns\|assert_no_phi\|PHI_COLUMNS" src/ tests/ pipelines/` — every remaining hit should be inside `databricks_delta.py` itself, `medallion_job.py`, or the new test files from this plan).

- [ ] **Step 8: Commit**

```bash
git add src/vitals/backends/databricks_delta.py src/vitals/medallion_job.py tests/test_databricks_delta.py
git commit -m "feat(security): check all 8 silver tables for PHI in the Databricks engine (ADR 0011)"
```

---

## Task 5: Wire the Databricks-path check into CI (hermetic, no cluster)

**Files:**
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: nothing new — `tests/test_databricks_delta.py` from Task 4 already contains the proof
  (`test_assert_all_silver_no_phi_catches_a_leak_without_a_cluster`) that the Databricks-path check
  has teeth, and it's hermetic (uses `_FakeSpark`, no `databricks.connect`/cluster needed).

This task is almost entirely already done by Task 4: `uv run pytest -q` (the CI step "Unit tests
(pytest)") already runs `tests/test_databricks_delta.py`, since pytest auto-discovers everything
under `tests/`. The only remaining gap is that `databricks_delta.py` imports `databricks.sdk`/
`databricks.connect` **lazily inside functions** (`_spark()`, `_upload_landing()`), but importing
the *module itself* (`from vitals.backends import databricks_delta as dx`) must not fail even
without those packages installed — verify this holds, since CI's `uv sync --extra dev --extra local
--extra dq` may not install the `databricks` extras.

- [ ] **Step 1: Check whether `databricks.connect`/`databricks.sdk` are installed under CI's actual
extras**

Run: `grep -n "databricks" pyproject.toml`
Read the output to find which optional-dependency group (if any) contains `databricks-connect` /
`databricks-sdk`, and confirm CI's `uv sync --extra dev --extra local --extra dq` (from
`.github/workflows/ci.yml`) does **not** include that group.

- [ ] **Step 2: Write a smoke-test confirming the module imports without the databricks extras**

Add to `tests/test_databricks_delta.py`:

```python
def test_module_imports_without_a_live_workspace():
    """Importing databricks_delta must not require databricks.connect/sdk — only calling _spark()
    or _upload_landing() does (they lazy-import). This is what lets assert_all_silver_no_phi run
    hermetically in CI without a cluster."""
    import importlib

    from vitals.backends import databricks_delta

    importlib.reload(databricks_delta)  # re-exercise the module-level import path
    assert hasattr(databricks_delta, "assert_all_silver_no_phi")
```

- [ ] **Step 3: Run it**

Run: `uv run pytest tests/test_databricks_delta.py::test_module_imports_without_a_live_workspace -v`
Expected: PASS. If it fails with `ModuleNotFoundError: databricks`, the module has a top-level (not
lazy) `import databricks...` somewhere — find it with `grep -n "^import databricks\|^from databricks"
src/vitals/backends/databricks_delta.py` and report back before proceeding (this would mean Task 4's
design assumption was wrong and the CI step below can't work as planned; do not silently work around
it).

- [ ] **Step 4: Add a CI step comment clarifying coverage (no new step needed — pytest already covers it)**

Modify `.github/workflows/ci.yml` — update the existing "Unit tests (pytest)" step's context by
adding a comment above it (the step itself is unchanged, since pytest already discovers
`tests/test_databricks_delta.py`):

Find:
```yaml
      - name: Unit tests (pytest)
        run: uv run pytest -q
```

Replace with:
```yaml
      # Includes tests/test_databricks_delta.py's assert_all_silver_no_phi tests — the Databricks
      # engine's PHI check is proven here with a fake SparkSession, no live cluster needed (ADR 0011).
      - name: Unit tests (pytest)
        run: uv run pytest -q
```

- [ ] **Step 5: Run the full local CI-equivalent sequence to confirm everything still gates correctly**

Run: `uv run ruff check . && uv run pytest -q && make build && make dq`
Expected: all four commands exit 0, matching the CI job's four steps in order.

- [ ] **Step 6: Commit**

```bash
git add .github/workflows/ci.yml tests/test_databricks_delta.py
git commit -m "test(ci): document that the Databricks PHI check is CI-covered without a cluster (ADR 0011)"
```

---

## Task 6: ADR 0011 + ADR 0002 History cross-reference

**Files:**
- Create: `docs/adr/0011-unified-phi-column-gate.md`
- Modify: `docs/adr/0002-de-identification-at-silver.md` (History section)

**Interfaces:** None — documentation only, no code interfaces.

- [ ] **Step 1: Write `docs/adr/0011-unified-phi-column-gate.md`**

```markdown
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
```

- [ ] **Step 2: Add a History entry to ADR 0002**

In `docs/adr/0002-de-identification-at-silver.md`, under the existing `## History` section (after
the `2026-07-14 — **gap closed.**` entry), add:

```markdown
- 2026-07-23 — **coverage broadened.** The PHI-column assertion this ADR established was duplicated
  across engines, covered ~5 of 18 Safe Harbor categories, and checked only `silver.patient`. See
  ADR 0011 — one shared 18-category check (`vitals.phi`), applied to all 8 silver tables, on both
  engines, CI-verified.
```

- [ ] **Step 3: Verify the ADR numbering is correct**

Run: `ls docs/adr/`
Expected: `0011-unified-phi-column-gate.md` is the highest-numbered file (confirm `0010-*` exists and
there is no pre-existing `0011-*`).

- [ ] **Step 4: Commit**

```bash
git add docs/adr/0011-unified-phi-column-gate.md docs/adr/0002-de-identification-at-silver.md
git commit -m "docs(adr): record ADR 0011 — unified Safe Harbor PHI-column gate"
```

---

## Task 7: Full-suite verification + README check

**Files:**
- Read-only check: `README.md` (only modify if it references the old PHI mechanism)

**Interfaces:** None.

- [ ] **Step 1: Search for any other reference to the old mechanism**

Run: `grep -rn "silver_patient_columns\|PHI_COLUMNS\b" --include="*.py" --include="*.md" .`
Expected: no hits outside `tests/` and the files already modified in Tasks 3-4 (the old
`PHI_COLUMNS` set and `silver_patient_columns()` function should be fully gone from
`databricks_delta.py`). If `README.md` or any other doc mentions the PHI check mechanism by name,
read that section and update it to reflect `vitals.phi` / ADR 0011 if it's materially describing the
old (single-table, 5-category) design.

- [ ] **Step 2: Run the complete verification sequence**

Run: `uv run ruff check . && uv run pytest -q && make build && make dq`
Expected: all exit 0. This is the same sequence CI runs — confirms the whole change is
green end-to-end, matching this plan's spec-derived acceptance criteria (all 8 silver tables
checked, both engines, 18 categories, CI-provable Databricks path, zero false positives).

- [ ] **Step 3: Confirm git status is clean and all commits are present**

Run: `git log --oneline -8`
Expected: 6 commits from Tasks 1-6 (plus any README fix from Step 1 of this task), each with a
clear scope, no uncommitted changes (`git status --short` empty).

No commit for this task itself — it's verification-only, not a deliverable.
