"""Unit tests for the bronze Delta parity check (pure logic, no workspace needed).

The row-count parity gate is the acceptance criterion for the Delta-on-UC bronze slice, so the
comparison logic itself is tested here — separate from the I/O that talks to Databricks.
"""
from vitals.backends import databricks_delta as dd


def test_parity_report_all_match():
    local = {"patients": 629, "observations": 6872}
    remote = {"patients": 629, "observations": 6872}
    report = dd.parity_report(local, remote)
    assert dd.all_match(report)
    assert report["patients"] == {"local": 629, "remote": 629, "match": True}


def test_parity_report_detects_mismatch():
    report = dd.parity_report({"patients": 629}, {"patients": 628})
    assert not dd.all_match(report)
    assert report["patients"]["match"] is False


def test_parity_report_flags_missing_remote_table():
    # a source present locally but never written remotely must NOT pass
    report = dd.parity_report({"claims": 1510}, {})
    assert report["claims"] == {"local": 1510, "remote": None, "match": False}
    assert not dd.all_match(report)


def test_all_match_empty_is_false():
    # an empty report is a failure, not a vacuous pass (nothing was verified)
    assert dd.all_match({}) is False


def test_sources_cover_the_eight_bronze_entities():
    assert set(dd.SOURCES) == {
        "patients", "encounters", "conditions", "observations",
        "notes", "claims", "pro_surveys", "wearables",
    }


# ---- silver: PHI boundary + transform contract ------------------------------------------------

def test_assert_no_phi_passes_for_deidentified_columns():
    dd.assert_no_phi(["patient_key", "gender", "age", "_date_shift_days"])  # must not raise


def test_assert_no_phi_raises_when_identifier_leaks():
    import pytest
    for leak in ("name", "identifier", "address", "birthDate", "ssn"):
        with pytest.raises(AssertionError):
            dd.assert_no_phi(["patient_key", leak])


def test_silver_statements_build_all_tables_patient_first():
    stmts = dd._silver_statements()
    names = [name for name, _ in stmts]
    assert names[0] == "patient", "patient must build first — other tables join to it"
    assert set(names) == set(dd.SILVER_TABLES)


def test_silver_patient_drops_phi_and_keeps_surrogate_key():
    sql = dict(dd._silver_statements())["patient"]
    assert "md5(id) AS patient_key" in sql            # hashed surrogate key
    assert "_date_shift_days" in sql                  # interval-preserving date shift
    for phi in ("name", "address", "identifier", "birthDate"):
        assert f" {phi}" not in sql.replace("birthDate IS NULL", "")  # not selected (birthDate only in age calc)


def test_silver_condition_embeds_vocab_recovery():
    sql = dict(dd._silver_statements())["condition"]
    assert "low back pain" in sql and "M54.5" in sql  # free-text -> ICD-10 recovery is wired
