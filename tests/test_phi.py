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


def test_every_category_pattern_resolves_to_its_own_category():
    """Each category's own patterns must classify as that category, not get shadowed
    by an earlier category in SAFE_HARBOR_PATTERNS' iteration order."""
    for category, patterns in phi.SAFE_HARBOR_PATTERNS.items():
        for pattern in patterns:
            with pytest.raises(AssertionError, match=category):
                phi.assert_no_phi("some_table", {"patient_key", pattern})


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


# The real current column sets for every silver table (src/vitals/lakehouse.py /
# src/vitals/backends/databricks_delta.py _silver_statements()). Hard-coded here (not imported)
# so this test stays hermetic — no DuckDB build or Spark session required.
# Mirror of REAL_SILVER_COLUMNS in tests/test_databricks_delta.py (set vs list only) — that file's
# test_real_silver_columns_matches_test_phi fails loudly if the two drift apart.
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
