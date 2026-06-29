"""Phase 4 — data/feature drift monitoring (MLOps), local DuckDB path.

Distributions shift in healthcare (new cohorts, seasonality, device changes), silently degrading
models. This loads the gold feature table and computes the **Population Stability Index (PSI)** per
feature between a reference and a current window, flagging drift two ways: a natural split (should be
stable) and an injected shift (should fire).

The PSI math itself lives in `vitals.drift` (engine-agnostic, no duckdb) so the *same* logic runs on
Databricks as a scheduled job (`pipelines/drift_job.py`). This module is just the local I/O around it.
"""
from __future__ import annotations

import json
from pathlib import Path

from vitals.drift import build_report

ROOT = Path(__file__).resolve().parents[2]
DB = ROOT / "data" / "vitals.duckdb"
OUT = ROOT / "data" / "drift_report.json"


def run() -> dict:
    import duckdb  # lazy: only the local venv has duckdb (serverless/connect venvs do not)

    con = duckdb.connect(str(DB))
    feats = con.execute("SELECT * FROM gold.patient_features").df()
    con.close()

    report = build_report(feats)
    OUT.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    return report


if __name__ == "__main__":
    run()
