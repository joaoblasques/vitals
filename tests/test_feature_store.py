import importlib.util
from pathlib import Path

import pandas as pd
import pytest

from vitals import feature_store as fs

_PARQUET = Path(__file__).resolve().parents[1] / "data" / "gold" / "patient_features.parquet"


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


def test_parity_exactly_one_nan_is_mismatch():
    """null-vs-value must flag as mismatch — the most dangerous silent-pass edge case."""
    off = _offline()  # p2 has NaN for mean_glucose_mgdl
    got = off[["patient_key", *fs.FEATURES]].copy()
    # replace the NaN with a real value: offline=NaN, retrieved=real → must mismatch
    got.loc[got["patient_key"] == "p2", "mean_glucose_mgdl"] = 99.9
    res = fs.parity(got, off, ["p1", "p2"])
    assert res["mean_glucose_mgdl"] is False
    assert res["all_match"] is False


def test_parity_retrieved_null_but_offline_has_value_is_mismatch():
    """The inverse (and operationally more dangerous) direction: Feast silently dropped a value and
    returns null where the offline parquet had a real number — must flag as mismatch, never pass."""
    off = _offline()
    got = off[["patient_key", *fs.FEATURES]].copy()
    got.loc[got["patient_key"] == "p1", "mean_pain"] = float("nan")  # offline=6.0, retrieved=NaN
    res = fs.parity(got, off, ["p1", "p2"])
    assert res["mean_pain"] is False
    assert res["all_match"] is False


# ---------------------------------------------------------------------------
# Gated integration test — skips in CI (no feast extra) and when parquet is missing
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    importlib.util.find_spec("feast") is None or not _PARQUET.exists(),
    reason="needs the `feast` extra + `make run` (gated; skips in CI)",
)
def test_feast_online_and_historical_match_offline():
    """Real Feast retrieval must reproduce the offline parquet (online + point-in-time historical).
    Gated: skips in CI (no `feast` extra) and when the parquet isn't built."""
    result = fs.demo(n=5)
    assert result["online_parity"]["all_match"] is True, result["online_parity"]
    assert result["historical_parity"]["all_match"] is True, result["historical_parity"]
