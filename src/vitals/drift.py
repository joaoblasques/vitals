"""Pure drift math — the PSI core shared by the local monitor and the Databricks job.

No I/O, no engine: takes a per-patient feature DataFrame and returns the drift report dict. Kept
dependency-light (numpy + pandas only, NO duckdb/spark) so it imports cleanly on serverless and the
*same* math runs everywhere — the local monitor (`vitals.monitoring`), the databricks-connect dev
path, and the scheduled job (`pipelines/drift_job.py`) all call `build_report`, so PSI can't drift
between engines.

PSI bands (industry standard): < 0.1 stable · 0.1–0.2 moderate · > 0.2 significant.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# The features watched for population shift (subset of the gold feature table).
MONITORED = ["age", "mean_pain", "mean_adherence", "mean_odi", "mean_active_min",
             "mean_steps", "total_paid", "mean_glucose_mgdl"]


def psi(ref: pd.Series, cur: pd.Series, bins: int = 10) -> float:
    """Population Stability Index between a reference and a current distribution (quantile bins)."""
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


def band(p: float) -> str:
    return "significant" if p > 0.2 else "moderate" if p > 0.1 else "stable"


def scan(ref: pd.DataFrame, cur: pd.DataFrame) -> dict:
    """PSI + band for every MONITORED feature present in both frames."""
    out = {}
    for col in MONITORED:
        if col in ref.columns and col in cur.columns:
            p = round(psi(ref[col], cur[col]), 4)
            out[col] = {"psi": p, "band": band(p)}
    return out


def build_report(feats: pd.DataFrame) -> dict:
    """Drift report from a per-patient feature frame.

    Splits patients (sorted by key) into a reference (first 60%) and current (last 40%) window:
      1. natural split (reference vs held-out current) — should be stable;
      2. an injected population shift (sicker, less active cohort) — should be flagged.
    Demonstrating both proves the monitor fires on real drift and stays quiet otherwise.
    """
    feats = feats.sort_values("patient_key").reset_index(drop=True)
    cut = int(len(feats) * 0.6)
    ref, cur = feats.iloc[:cut], feats.iloc[cut:].copy()

    stable = scan(ref, cur)

    # inject a population shift to prove the monitor fires (sicker, less active cohort)
    shifted = cur.copy()
    shifted["mean_pain"] = shifted["mean_pain"] + 1.5
    shifted["mean_odi"] = shifted["mean_odi"] + 12
    shifted["mean_active_min"] = shifted["mean_active_min"] * 0.7
    shifted_scan = scan(ref, shifted)

    return {
        "reference_n": int(len(ref)), "current_n": int(len(cur)),
        "stable_split": stable,
        "shifted_population": shifted_scan,
        "alerts": [f for f, v in shifted_scan.items() if v["band"] != "stable"],
    }
