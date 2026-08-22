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
