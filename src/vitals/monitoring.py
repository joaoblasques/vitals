"""Phase 4 — data/feature drift monitoring (MLOps).

Distributions shift in healthcare (new cohorts, seasonality, device changes), silently degrading
models. This computes the **Population Stability Index (PSI)** per feature between a reference
window and a current window, and flags drift. Demonstrated two ways:
  1. a natural split (reference vs held-out current) — should be stable;
  2. a shifted population (injected drift) — should be flagged.

PSI bands (industry standard): < 0.1 stable · 0.1–0.2 moderate · > 0.2 significant.
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DB = ROOT / "data" / "vitals.duckdb"
OUT = ROOT / "data" / "drift_report.json"

MONITORED = ["age", "mean_pain", "mean_adherence", "mean_odi", "mean_active_min",
             "mean_steps", "total_paid", "mean_glucose_mgdl"]


def _psi(ref: pd.Series, cur: pd.Series, bins: int = 10) -> float:
    ref, cur = ref.dropna(), cur.dropna()
    if ref.nunique() < 2:
        return 0.0
    edges = np.unique(np.quantile(ref, np.linspace(0, 1, bins + 1)))
    edges[0], edges[-1] = -np.inf, np.inf
    r = np.histogram(ref, edges)[0] / len(ref)
    c = np.histogram(cur, edges)[0] / len(cur)
    eps = 1e-6
    r, c = np.clip(r, eps, None), np.clip(c, eps, None)
    return float(np.sum((c - r) * np.log(c / r)))


def _band(psi: float) -> str:
    return "significant" if psi > 0.2 else "moderate" if psi > 0.1 else "stable"


def _scan(ref: pd.DataFrame, cur: pd.DataFrame) -> dict:
    out = {}
    for col in MONITORED:
        if col in ref.columns and col in cur.columns:
            p = round(_psi(ref[col], cur[col]), 4)
            out[col] = {"psi": p, "band": _band(p)}
    return out


def run() -> dict:
    con = duckdb.connect(str(DB))
    feats = con.execute("SELECT * FROM gold.patient_features").df()
    con.close()

    # reference = first 60% of patients; current = remaining 40%
    feats = feats.sort_values("patient_key").reset_index(drop=True)
    cut = int(len(feats) * 0.6)
    ref, cur = feats.iloc[:cut], feats.iloc[cut:].copy()

    stable = _scan(ref, cur)

    # inject a population shift to prove the monitor fires (sicker, less active cohort)
    shifted = cur.copy()
    shifted["mean_pain"] = shifted["mean_pain"] + 1.5
    shifted["mean_odi"] = shifted["mean_odi"] + 12
    shifted["mean_active_min"] = shifted["mean_active_min"] * 0.7
    shifted_scan = _scan(ref, shifted)

    report = {
        "reference_n": int(len(ref)), "current_n": int(len(cur)),
        "stable_split": stable,
        "shifted_population": shifted_scan,
        "alerts": [f for f, v in shifted_scan.items() if v["band"] != "stable"],
    }
    OUT.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    return report


if __name__ == "__main__":
    run()
