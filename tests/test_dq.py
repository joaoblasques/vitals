import importlib.util

import pandas as pd
import pytest

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
    assert ("patient", "not_null", "patient_key") in have
    assert ("patient", "unique", "patient_key") in have
    assert ("pro", "between", "score") in have
    assert ("wearable_daily", "between", "steps") in have
    # completeness: every silver table asserts row_count >= 1 (no vacuous pass on empty data)
    for t in ("condition", "observation", "patient", "pro", "wearable_daily"):
        assert (t, "row_count_min", None) in have


def test_glucose_unit_expectation_is_filtered_to_glucose():
    spec = dq.expectations_spec()
    unit = next(e for e in spec if e["table"] == "observation" and e["column"] == "unit_std")
    assert unit["where"] == {"metric": "glucose"}
    assert unit["value_set"] == ["mg/dL"]


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
    bad["observation"] = pd.DataFrame(                    # a glucose reading in the wrong unit
        {"patient_key": ["p1"], "metric": ["glucose"], "unit_std": ["mmol/L"]})
    result = dq.validate(bad)
    assert result["success"] is False
    failed = {(r["table"], r["type"]) for r in result["results"] if not r["success"]}
    assert ("condition", "in_set") in failed
    assert ("patient", "columns_match_set") in failed
    assert ("observation", "in_set") in failed           # glucose unit_std must be mg/dL (where-filter path)


@pytest.mark.skipif(
    importlib.util.find_spec("great_expectations") is None,
    reason="needs the `dq` extra (great-expectations); runs in CI where it's installed",
)
def test_validate_flags_an_empty_silver_table():
    """The gate must not greenlight empty silver — a zero-row table fails its row_count expectation."""
    silver = {
        "condition": pd.DataFrame({"patient_key": [], "icd10_code": []}),   # EMPTY
        "observation": pd.DataFrame({"patient_key": ["p1"], "metric": ["pain"], "unit_std": ["score"]}),
        "patient": pd.DataFrame({"patient_key": ["p1"], "gender": ["m"], "age": [70], "_date_shift_days": [3]}),
        "pro": pd.DataFrame({"patient_key": ["p1"], "score": [40]}),
        "wearable_daily": pd.DataFrame({"patient_key": ["p1"], "steps": [8000]}),
    }
    result = dq.validate(silver)
    assert result["success"] is False
    failed = {(r["table"], r["type"]) for r in result["results"] if not r["success"]}
    assert ("condition", "row_count_min") in failed
