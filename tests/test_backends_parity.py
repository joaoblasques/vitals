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
