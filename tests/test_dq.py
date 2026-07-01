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
