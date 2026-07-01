import pandas as pd
import pytest

from vitals import feature_store as fs


def test_features_are_the_eight_view_fields():
    assert fs.FEATURES == [
        "age", "mean_pain", "last_pain", "pain_trend",
        "mean_adherence", "mean_glucose_mgdl", "mean_hr", "n_observations",
    ]
    assert fs.REFS == [f"patient_surgery_risk_features:{f}" for f in fs.FEATURES]


def test_entity_df_shape():
    df = fs.entity_df(["p1", "p2"])
    assert list(df.columns) == ["patient_key", "event_timestamp"]
    assert df["patient_key"].tolist() == ["p1", "p2"]
    assert str(df["event_timestamp"].dt.tz) == "UTC"


def _offline():
    return pd.DataFrame({
        "patient_key": ["p1", "p2"],
        "age": [70, 55], "mean_pain": [6.0, 3.0], "last_pain": [7.0, 2.0],
        "pain_trend": [1.0, -1.0], "mean_adherence": [0.8, 0.5],
        "mean_glucose_mgdl": [110.0, float("nan")], "mean_hr": [72.0, 66.0],
        "n_observations": [12, 8],
    })


def test_parity_all_match_when_equal_null_aware():
    off = _offline()
    got = off[["patient_key", *fs.FEATURES]].copy()  # identical incl. the NaN glucose for p2
    res = fs.parity(got, off, ["p1", "p2"])
    assert res["all_match"] is True
    assert res["mean_glucose_mgdl"] is True  # NaN == NaN counts as match


def test_parity_flags_a_perturbed_value():
    off = _offline()
    got = off[["patient_key", *fs.FEATURES]].copy()
    got.loc[got["patient_key"] == "p1", "mean_pain"] = 6.5  # > tol
    res = fs.parity(got, off, ["p1", "p2"])
    assert res["mean_pain"] is False
    assert res["all_match"] is False
