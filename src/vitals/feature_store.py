"""Feast feature-store driver — the third gold store (ADR 0008). Pure helpers (constants, entity-df,
parity) are separated from lazy Feast I/O so the core is testable without Feast installed. Mirrors the
optional-extra / graceful-skip pattern of vitals.vector_index. Local sqlite online + file offline."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
REPO = ROOT / "ml" / "feature_store"
PARQUET = ROOT / "data" / "gold" / "patient_features.parquet"
VIEW = "patient_surgery_risk_features"
FEATURES = [
    "age", "mean_pain", "last_pain", "pain_trend",
    "mean_adherence", "mean_glucose_mgdl", "mean_hr", "n_observations",
]
REFS = [f"{VIEW}:{f}" for f in FEATURES]

# Deterministic, TTL-safe (FeatureView ttl = 90d): stamp, materialize end, and query time all sit in
# one 90-day window. tz-aware UTC throughout to avoid naive/aware timestamp mismatches.
EVENT_TS = "2026-01-01"
MATERIALIZE_END = "2026-03-01"
QUERY_TS = "2026-03-01"


def entity_df(keys, at: str = QUERY_TS) -> pd.DataFrame:
    """Entity dataframe for a point-in-time historical query: one row per key at time `at` (UTC)."""
    return pd.DataFrame({
        "patient_key": list(keys),
        "event_timestamp": pd.Timestamp(at, tz="UTC"),
    })


def _match(a, b, tol: float) -> bool:
    if pd.isna(a) and pd.isna(b):
        return True
    if pd.isna(a) or pd.isna(b):
        return False
    return abs(float(a) - float(b)) <= tol


def parity(retrieved: pd.DataFrame, offline: pd.DataFrame, keys, features=FEATURES, tol: float = 1e-3) -> dict:
    """Per-feature match between retrieved (patient_key + feature cols) and the offline parquet, for the
    given keys. NULL-aware, float-tolerant. Returns {feature: bool, ..., 'all_match': bool}."""
    r = retrieved.set_index("patient_key")
    o = offline.set_index("patient_key")
    out = {f: all(_match(r.loc[k, f], o.loc[k, f], tol) for k in keys) for f in features}
    out = {f: bool(v) for f, v in out.items()}
    out["all_match"] = all(out.values())
    return out
