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


def is_available() -> bool:
    import importlib.util
    return importlib.util.find_spec("great_expectations") is not None


def _read_silver() -> dict:
    import duckdb
    con = duckdb.connect(str(DB))
    tables = {t: con.execute(f"SELECT * FROM silver.{t}").df()
              for t in ("condition", "observation", "patient", "pro", "wearable_daily")}
    con.close()
    return tables


def _expectation(item: dict):
    import great_expectations as gx
    t, col = item["type"], item.get("column")
    if t == "not_null":
        return gx.expectations.ExpectColumnValuesToNotBeNull(column=col)
    if t == "in_set":
        return gx.expectations.ExpectColumnValuesToBeInSet(column=col, value_set=item["value_set"])
    if t == "unique":
        return gx.expectations.ExpectColumnValuesToBeUnique(column=col)
    if t == "between":
        return gx.expectations.ExpectColumnValuesToBeBetween(
            column=col, min_value=item["min"], max_value=item["max"])
    if t == "columns_match_set":
        return gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=item["column_set"], exact_match=True)
    raise ValueError(f"unknown expectation type {t!r}")


def validate(silver: dict | None = None) -> dict:
    """Run the silver expectation suite with GE (ephemeral context, one pandas batch per expectation —
    filtered where a `where` clause is given, e.g. glucose-only unit check). Returns an aggregate result.
    Pass `silver` (dict of table->DataFrame) to validate injected data; default reads the built silver."""
    import great_expectations as gx
    if silver is None:
        silver = _read_silver()
    ctx = gx.get_context(mode="ephemeral")
    ds = ctx.data_sources.add_pandas("pandas")
    results = []
    for i, item in enumerate(expectations_spec()):
        df = silver[item["table"]]
        for k, v in item.get("where", {}).items():
            df = df[df[k] == v]
        asset = ds.add_dataframe_asset(name=f"a{i}_{item['table']}")
        bd = asset.add_batch_definition_whole_dataframe(f"b{i}")
        batch = bd.get_batch(batch_parameters={"dataframe": df})
        res = batch.validate(_expectation(item))
        results.append({
            "table": item["table"], "type": item["type"],
            "column": item.get("column") or "columns",
            "success": bool(res.success),
        })
    n_failed = sum(1 for r in results if not r["success"])
    return {"success": n_failed == 0, "n_expectations": len(results),
            "n_failed": n_failed, "results": results}


def main() -> None:
    result = validate()
    OUT.write_text(json.dumps(result, indent=2))
    passed = result["n_expectations"] - result["n_failed"]
    print(f"GE silver DQ: {passed}/{result['n_expectations']} expectations passed")
    for r in result["results"]:
        if not r["success"]:
            print(f"  FAILED: {r['table']}.{r['column']} [{r['type']}]")
    sys.exit(0 if result["success"] else 1)


if __name__ == "__main__":
    main()
