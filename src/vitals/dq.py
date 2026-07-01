"""Great Expectations DQ gate for the silver layer (ADR 0009). Pure spec (vocab-derived value-sets +
a plain-data description of every expectation) is separated from the GE validation I/O, so the suite is
testable without GE installed. GE GATES silver in CI (dbt gates gold); it is NOT an optional demo."""
from __future__ import annotations

import json
import sys
from pathlib import Path

from vitals import vocab

ROOT = Path(__file__).resolve().parents[2]
DB = ROOT / "data" / "vitals.duckdb"
OUT = ROOT / "data" / "ge_validation.json"

VALID_METRICS = ["adherence", "glucose", "heart_rate", "other", "pain"]
SILVER_PATIENT_COLUMNS = ["patient_key", "gender", "age", "_date_shift_days"]
PRO_MIN, PRO_MAX = 0, 100
STEPS_MIN, STEPS_MAX = 0, 50000


def valid_icd10() -> list[str]:
    """The coded-vocabulary contract: the ICD-10 codes silver standardizes to (the display map's keys +
    the text-recovery targets), from version-controlled vocab. Sorted for deterministic value-sets."""
    return sorted(set(vocab.ICD_DISPLAY) | set(vocab.TEXT_TO_ICD.values()))


def expectations_spec() -> list[dict]:
    """Plain-data description of the silver DQ contract — the GE layer materializes each into a real
    gx.expectations.* object. Kept pure so the suite's coverage is unit-testable without GE."""
    return [
        {"table": "condition", "type": "not_null", "column": "icd10_code"},
        {"table": "condition", "type": "in_set", "column": "icd10_code", "value_set": valid_icd10()},
        {"table": "observation", "type": "in_set", "column": "metric", "value_set": VALID_METRICS},
        {"table": "observation", "type": "in_set", "column": "unit_std",
         "value_set": ["mg/dL"], "where": {"metric": "glucose"}},
        {"table": "patient", "type": "columns_match_set", "column": None,
         "column_set": SILVER_PATIENT_COLUMNS},
        {"table": "patient", "type": "not_null", "column": "patient_key"},
        {"table": "patient", "type": "unique", "column": "patient_key"},
        {"table": "pro", "type": "between", "column": "score", "min": PRO_MIN, "max": PRO_MAX},
        {"table": "wearable_daily", "type": "between", "column": "steps",
         "min": STEPS_MIN, "max": STEPS_MAX},
    ]
