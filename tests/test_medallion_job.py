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
