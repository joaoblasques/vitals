"""Unit tests for the PSI drift core + the Databricks drift wiring (pure logic, no workspace).

The drift report is the monitoring contract, so the PSI math and the parity comparison are tested
here — separate from the Spark I/O that reads the marts and writes vitals_gold.monitoring.
"""
import json
from pathlib import Path

import numpy as np
import pandas as pd

from vitals import drift
from vitals.backends import databricks_delta as dd

ROOT = Path(__file__).resolve().parents[1]


# ---- PSI core ---------------------------------------------------------------------------------

def test_psi_zero_for_identical_distributions():
    s = pd.Series(np.arange(100, dtype=float))
    assert drift.psi(s, s) == 0.0


def test_psi_grows_with_a_shifted_distribution():
    ref = pd.Series(np.arange(100, dtype=float))
    small = drift.psi(ref, ref + 5)
    big = drift.psi(ref, ref + 50)
    assert 0 < small < big  # more shift -> more PSI


def test_psi_constant_reference_is_safe():
    # a degenerate (single-value) reference must not blow up — returns 0, not NaN/inf
    assert drift.psi(pd.Series([1.0] * 50), pd.Series(np.arange(50, dtype=float))) == 0.0


def test_band_thresholds():
    assert drift.band(0.05) == "stable"
    assert drift.band(0.15) == "moderate"
    assert drift.band(0.25) == "significant"


def test_build_report_flags_injected_shift_but_not_natural_split():
    rng = np.random.default_rng(0)
    n = 600
    feats = pd.DataFrame({
        "patient_key": [f"p{i:04d}" for i in range(n)],
        "age": rng.normal(50, 10, n),
        "mean_pain": rng.normal(5, 1, n),
        "mean_adherence": rng.normal(0.8, 0.1, n),
        "mean_odi": rng.normal(40, 8, n),
        "mean_active_min": rng.normal(30, 5, n),
        "mean_steps": rng.normal(6000, 1000, n),
        "total_paid": rng.normal(2000, 500, n),
        "mean_glucose_mgdl": rng.normal(100, 15, n),
    })
    report = drift.build_report(feats)
    assert report["reference_n"] == 360 and report["current_n"] == 240
    # random split -> stable; the injected shift bumps pain/odi/active_min -> they must alert
    assert report["stable_split"]["mean_pain"]["band"] == "stable"
    assert {"mean_pain", "mean_odi", "mean_active_min"} <= set(report["alerts"])


# ---- regression: the refactor must reproduce the canonical local report -----------------------

def test_build_report_reproduces_committed_baseline():
    """The shared core must produce the exact report the local monitor committed (drift_report.json),
    proving the extraction into vitals.drift didn't change the math. Reads the feature parquet
    (`serve.py` writes the same data it loads into gold.patient_features)."""
    parquet = ROOT / "data" / "gold" / "patient_features.parquet"
    baseline_path = ROOT / "data" / "drift_report.json"
    if not parquet.exists() or not baseline_path.exists():
        import pytest
        pytest.skip("local feature parquet / baseline not present (run `make run` then `make monitor`)")

    feats = pd.read_parquet(parquet)
    report = drift.build_report(feats)
    baseline = json.loads(baseline_path.read_text())
    assert report["stable_split"] == baseline["stable_split"]
    assert report["shifted_population"] == baseline["shifted_population"]
    assert report["alerts"] == baseline["alerts"]


# ---- Databricks drift wiring (pure parts) -----------------------------------------------------

def test_drift_feature_sql_covers_all_monitored_columns():
    sql = dd.DRIFT_FEATURE_SQL
    # every monitored feature (minus patient_key) must be produced by the feature query
    for col in drift.MONITORED:
        assert col in sql, f"{col} missing from DRIFT_FEATURE_SQL"
    # same join shape as the local FEATURE_SQL: inner on observations, left on the rest
    assert "JOIN obs o USING (patient_key)" in sql
    assert sql.count("LEFT JOIN") == 3


def test_drift_rows_flattens_both_splits():
    report = {
        "stable_split": {"age": {"psi": 0.05, "band": "stable"}},
        "shifted_population": {"mean_pain": {"psi": 0.5, "band": "significant"}},
    }
    rows = dd.drift_rows(report)
    assert ("stable_split", "age", 0.05, "stable", False) in rows
    assert ("shifted_population", "mean_pain", 0.5, "significant", True) in rows


def test_drift_parity_matches_within_tolerance_and_flags_drift():
    local = {"stable_split": {"age": {"psi": 0.0794, "band": "stable"}},
             "shifted_population": {}}
    close = {"stable_split": {"age": {"psi": 0.0795, "band": "stable"}},
             "shifted_population": {}}
    far = {"stable_split": {"age": {"psi": 0.20, "band": "moderate"}},
           "shifted_population": {}}
    assert dd.drift_parity(local, close)["stable_split.age"]["match"] is True
    assert dd.drift_parity(local, far)["stable_split.age"]["match"] is False


def test_drift_parity_missing_feature_is_a_mismatch():
    local = {"stable_split": {"age": {"psi": 0.05, "band": "stable"}}, "shifted_population": {}}
    remote = {"stable_split": {}, "shifted_population": {}}
    assert dd.drift_parity(local, remote)["stable_split.age"]["match"] is False
