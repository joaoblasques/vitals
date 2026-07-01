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


# ---------------------------------------------------------------------------
# Lazy Feast I/O — only imported when feast is installed (feast extra).
# ---------------------------------------------------------------------------

def is_available() -> bool:
    """True when Feast is importable AND the offline parquet exists (else the demo skips)."""
    try:
        import feast  # noqa: F401
    except ImportError:
        return False
    return PARQUET.exists()


def _store():
    from feast import FeatureStore
    return FeatureStore(repo_path=str(REPO))


def _feast_bin() -> str:
    # the `feast` CLI installed alongside the running interpreter (avoids PATH surprises)
    return str(Path(sys.executable).parent / "feast")


def apply_materialize() -> None:
    """Register the repo (feast apply) + load offline -> online for [EVENT_TS, MATERIALIZE_END] (tz-aware UTC).

    Uses store.materialize() with explicit start/end so the window is deterministic regardless of
    current wall-clock time (materialize_incremental starts from registry last-updated which equals
    now when the registry is fresh, producing an empty window when now > MATERIALIZE_END).
    """
    import subprocess
    from datetime import datetime, timezone
    subprocess.run([_feast_bin(), "apply"], cwd=str(REPO), check=True)
    start = datetime.fromisoformat(EVENT_TS).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(MATERIALIZE_END).replace(tzinfo=timezone.utc)
    _store().materialize(start_date=start, end_date=end)


def online_features(store, keys) -> pd.DataFrame:
    d = store.get_online_features(
        features=REFS, entity_rows=[{"patient_key": k} for k in keys]
    ).to_dict()
    return pd.DataFrame(d)


def historical_features(store, keys) -> pd.DataFrame:
    return store.get_historical_features(entity_df=entity_df(keys), features=REFS).to_df()


def demo(n: int = 5) -> dict:
    """apply + materialize, then online + point-in-time historical retrieval, each parity-checked
    against the offline parquet for n sampled patients (deterministic: first n sorted keys)."""
    offline = pd.read_parquet(PARQUET)
    keys = sorted(offline["patient_key"].tolist())[:n]
    apply_materialize()
    store = _store()
    online = online_features(store, keys)
    hist = historical_features(store, keys)
    return {
        "store": "feast (sqlite online + file offline)",
        "n_served": len(keys),
        "online_parity": parity(online, offline, keys),
        "historical_parity": parity(hist, offline, keys),
    }


def main() -> None:
    cmd = sys.argv[1] if len(sys.argv) > 1 else "demo"
    if cmd == "apply":
        import subprocess
        subprocess.run([_feast_bin(), "apply"], cwd=str(REPO), check=True)
    elif cmd == "materialize":  # pragma: no cover
        apply_materialize()
    elif cmd == "online":
        print(online_features(_store(), sys.argv[2:]))
    elif cmd == "historical":
        print(historical_features(_store(), sys.argv[2:]))
    else:
        print(json.dumps(demo(), indent=2, default=str))


if __name__ == "__main__":
    main()
