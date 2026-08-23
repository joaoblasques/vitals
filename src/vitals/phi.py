"""Canonical HIPAA Safe Harbor identifier check (45 CFR 164.514(b)(2)) — the single source of
truth for "does this column look like PHI", shared by every silver-writing engine (DuckDB,
Databricks) so the two can never drift again (ADR 0011).

Column-name/pattern matching only — this does NOT scan cell values for embedded identifiers (e.g.
an SSN inside a free-text note). That is a documented, deliberate gap; see ADR 0011.
"""
from __future__ import annotations

# 18 Safe Harbor categories -> lowercase substrings that indicate that category in a column name.
SAFE_HARBOR_PATTERNS: dict[str, set[str]] = {
    "names": {"name", "patient_name", "first_name", "last_name"},
    "ip_addresses": {"ip_address", "ip_addr"},
    "geographic_subdivisions": {"address", "street", "city", "zip", "postal"},
    "dates": {"birthdate", "birth_date", "dob", "admit_date", "discharge_date", "death_date"},
    "telephone_numbers": {"phone", "telephone", "mobile_number"},
    "fax_numbers": {"fax"},
    "email_addresses": {"email"},
    "ssn": {"ssn", "social_security"},
    "medical_record_numbers": {"medical_record_number", "mrn"},
    "health_plan_beneficiary_numbers": {"health_plan_beneficiary", "beneficiary_id", "beneficiary_number"},
    "account_numbers": {"account_number", "account_no"},
    "certificate_license_numbers": {"license_number", "certificate_number"},
    "vehicle_identifiers": {"vehicle_vin", "vin", "license_plate"},
    "device_identifiers": {"device_serial", "device_id", "serial_number"},
    "biometric_identifiers": {"fingerprint", "biometric", "retina", "voiceprint"},
    "full_face_photos": {"photo", "headshot", "face_image"},
    "urls": {"url", "website"},
    "other_unique_identifiers": {"identifier", "unique_id"},
}


def assert_no_phi(table: str, columns: set[str]) -> None:
    """Raise if any column in `columns` matches a Safe Harbor identifier pattern.

    Deterministic: categories checked in SAFE_HARBOR_PATTERNS order, columns in sorted order, so the
    same offending input always produces the same error message (the first match).
    """
    lowered = {c: c.lower() for c in columns}
    for category, patterns in SAFE_HARBOR_PATTERNS.items():
        for col in sorted(columns):
            col_lower = lowered[col]
            if any(pattern in col_lower for pattern in patterns):
                # Raised explicitly (not via `assert`) so `python -O` can't silently strip this gate.
                raise AssertionError(
                    f"PHI leaked into silver.{table}: column '{col}' matches "
                    f"Safe Harbor category '{category}'"
                )
