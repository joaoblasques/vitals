import pytest

import test_phi
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


# Mirror of REAL_SILVER_COLUMNS in tests/test_phi.py (set vs list only — Databricks' .columns
# API returns a list). test_real_silver_columns_matches_test_phi below fails loudly if they drift.
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


def test_real_silver_columns_matches_test_phi():
    """Guards against tests/test_phi.py and this file's REAL_SILVER_COLUMNS drifting apart."""
    as_sets = {table: set(cols) for table, cols in REAL_SILVER_COLUMNS.items()}
    assert as_sets == test_phi.REAL_SILVER_COLUMNS


def test_assert_all_silver_no_phi_passes_real_schemas():
    fake_spark = _FakeSpark(REAL_SILVER_COLUMNS)
    dx.assert_all_silver_no_phi(fake_spark)  # must not raise


def test_assert_all_silver_no_phi_checks_every_table():
    checked = []
    tables = dict(REAL_SILVER_COLUMNS)
    fake_spark = _FakeSpark(tables)

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


def test_module_imports_without_a_live_workspace():
    """Importing databricks_delta must not require databricks.connect/sdk — only calling _spark()
    or _upload_landing() does (they lazy-import). This is what lets assert_all_silver_no_phi run
    hermetically in CI without a cluster."""
    import importlib

    from vitals.backends import databricks_delta

    importlib.reload(databricks_delta)  # re-exercise the module-level import path
    assert hasattr(databricks_delta, "assert_all_silver_no_phi")
