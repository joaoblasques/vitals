# Feast Feature Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the scaffolded Feast feature store real — apply + materialize the offline parquet into the sqlite online store, and demonstrate online (inference) and point-in-time historical (leakage-safe training) retrieval, each parity-proven against the offline parquet.

**Architecture:** A `src/vitals/feature_store.py` driver (pure helpers separated from lazy Feast I/O, mirroring `vector_index.py`) runs against the existing `ml/feature_store/` repo. `serve.py` stamps a deterministic `event_timestamp` on the feature parquet and calls the demo lazily (pgvector-style skip when Feast is absent). Feast is an optional extra; the hermetic CI gate and clone-and-run are untouched.

**Tech Stack:** Feast (local sqlite online + file offline), pandas, DuckDB (upstream), pytest.

## Global Constraints

- **Optional `feast` extra** (already `feast>=0.40` in pyproject). `make build` runs `--no-serve` and never touches Feast; the demo + integration test **skip when Feast isn't installed** (importorskip / `is_available()`), exactly like the pgvector / MetricFlow pattern. Clone-and-run + CI unchanged.
- **Deterministic, TTL-safe timestamps** (or retrieval silently returns null): FeatureView ttl = 90 days. Use **`EVENT_TS = "2026-01-01"`**, **`MATERIALIZE_END = "2026-03-01"`**, **`QUERY_TS = "2026-03-01"`** — all within one 90-day window, all fixed. Use **tz-aware UTC** timestamps everywhere (offline stamp, entity df, materialize end) to avoid naive/aware mismatch.
- **Parity is the correctness contract:** online + historical retrieval must equal the offline parquet values for the sampled patients (float tolerance `1e-3`, **NULL-aware** — `mean_glucose_mgdl`/`mean_hr` can be NaN).
- **8 FeatureView fields** (verbatim, order matters for `FEATURES`): `age`, `mean_pain`, `last_pain`, `pain_trend`, `mean_adherence`, `mean_glucose_mgdl`, `mean_hr`, `n_observations`. Feature refs are `patient_surgery_risk_features:<field>`.
- **Model stays on the parquet** (do not repoint `_train_model`). **Local only** (no Feast on Databricks).
- Tests import `vitals` via pytest `pythonpath = ["src"]`; run hermetic tests with `uv run --extra dev --extra local`, Feast ones with `--extra feast` added.

---

### Task 1: pure helpers + constants (`feature_store.py`), hermetic

The Feast-independent core: constants, the entity-dataframe builder, and the NULL-aware parity comparator — all unit-testable without Feast installed.

**Files:**
- Create: `src/vitals/feature_store.py`
- Test: `tests/test_feature_store.py`

**Interfaces:**
- Produces: `FEATURES: list[str]` (the 8 fields), `REFS: list[str]`, `EVENT_TS`/`MATERIALIZE_END`/`QUERY_TS: str`, `REPO`/`PARQUET: Path`; `entity_df(keys, at=QUERY_TS) -> pd.DataFrame`; `parity(retrieved: pd.DataFrame, offline: pd.DataFrame, keys, features=FEATURES, tol=1e-3) -> dict` (returns `{feature: bool, ..., "all_match": bool}`).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_feature_store.py`:

```python
import pandas as pd
import pytest

from vitals import feature_store as fs


def test_features_are_the_eight_view_fields():
    assert fs.FEATURES == [
        "age", "mean_pain", "last_pain", "pain_trend",
        "mean_adherence", "mean_glucose_mgdl", "mean_hr", "n_observations",
    ]
    assert fs.REFS == [f"patient_surgery_risk_features:{f}" for f in fs.FEATURES]


def test_entity_df_shape():
    df = fs.entity_df(["p1", "p2"])
    assert list(df.columns) == ["patient_key", "event_timestamp"]
    assert df["patient_key"].tolist() == ["p1", "p2"]
    assert str(df["event_timestamp"].dt.tz) == "UTC"


def _offline():
    return pd.DataFrame({
        "patient_key": ["p1", "p2"],
        "age": [70, 55], "mean_pain": [6.0, 3.0], "last_pain": [7.0, 2.0],
        "pain_trend": [1.0, -1.0], "mean_adherence": [0.8, 0.5],
        "mean_glucose_mgdl": [110.0, float("nan")], "mean_hr": [72.0, 66.0],
        "n_observations": [12, 8],
    })


def test_parity_all_match_when_equal_null_aware():
    off = _offline()
    got = off[["patient_key", *fs.FEATURES]].copy()  # identical incl. the NaN glucose for p2
    res = fs.parity(got, off, ["p1", "p2"])
    assert res["all_match"] is True
    assert res["mean_glucose_mgdl"] is True  # NaN == NaN counts as match


def test_parity_flags_a_perturbed_value():
    off = _offline()
    got = off[["patient_key", *fs.FEATURES]].copy()
    got.loc[got["patient_key"] == "p1", "mean_pain"] = 6.5  # > tol
    res = fs.parity(got, off, ["p1", "p2"])
    assert res["mean_pain"] is False
    assert res["all_match"] is False
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --extra dev --extra local pytest tests/test_feature_store.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'vitals.feature_store'`.

- [ ] **Step 3: Create `src/vitals/feature_store.py` (pure core only)**

```python
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --extra dev --extra local pytest tests/test_feature_store.py -q`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/vitals/feature_store.py tests/test_feature_store.py
git commit -m "feat(feast): feature-store pure helpers (entity_df + null-aware parity)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01F1hdqzju3WbgEALYreWUjk"
```

---

### Task 2: Feast I/O + serve wiring + event_timestamp + Makefile/gitignore

The lazy Feast driver, the deterministic offline timestamp, and the pgvector-style serve wiring — verified end-to-end locally with the `feast` extra.

**Files:**
- Modify: `src/vitals/feature_store.py` (append lazy I/O + `main`)
- Modify: `src/vitals/serve.py` (event_timestamp on parquet; `_feature_store_demo` in `run()`)
- Modify: `ml/feature_store/features.py` (verify fields; change only on mismatch)
- Modify: `Makefile` (`feast-demo`), `.gitignore` (Feast artifacts)

**Interfaces:**
- Consumes: `FEATURES`/`REFS`/`REPO`/`PARQUET`/`entity_df`/`parity` (Task 1).
- Produces: `is_available() -> bool`; `demo(n=5) -> dict` (`{store, n_served, online_parity, historical_parity}`); `apply_materialize()`, `online_features(store, keys) -> pd.DataFrame`, `historical_features(store, keys) -> pd.DataFrame`, `main()`.

- [ ] **Step 1: Append the lazy Feast I/O to `src/vitals/feature_store.py`**

```python
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
    """Register the repo (feast apply) + load offline -> online up to MATERIALIZE_END (tz-aware UTC)."""
    import subprocess
    from datetime import datetime, timezone
    subprocess.run([_feast_bin(), "apply"], cwd=str(REPO), check=True)
    end = datetime.fromisoformat(MATERIALIZE_END).replace(tzinfo=timezone.utc)
    _store().materialize_incremental(end_date=end)


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
    elif cmd == "materialize":
        apply_materialize()
    elif cmd == "online":
        print(online_features(_store(), sys.argv[2:]))
    elif cmd == "historical":
        print(historical_features(_store(), sys.argv[2:]))
    else:
        print(json.dumps(demo(), indent=2, default=str))


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Stamp `event_timestamp` on the parquet + wire the demo in `serve.py`**

In `src/vitals/serve.py`, in `run()` section 1, change the parquet write to add the tz-aware UTC timestamp, then compute the feast demo result:

```python
    # ---------- 1. FEATURE STORE (offline table + parquet) ----------
    feats = con.execute(FEATURE_SQL).df()
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    con.execute("CREATE OR REPLACE TABLE gold.patient_features AS SELECT * FROM feats")
    feats["event_timestamp"] = pd.Timestamp("2026-01-01", tz="UTC")   # Feast FileSource timestamp (ADR 0008)
    feats.to_parquet(GOLD / "patient_features.parquet", index=False)
    feast_demo = _feature_store_demo()
```

Add the helper near `_rag_demo` (mirrors its lazy try/fallback):

```python
def _feature_store_demo() -> dict:
    """Real Feast online + point-in-time historical retrieval when the `feast` extra is installed;
    a skip note otherwise (offline parquet is always written above). See ADR 0008."""
    try:
        from vitals import feature_store as fs
        if not fs.is_available():
            return {"store": "feast (skipped: extra not installed)"}
        return fs.demo()
    except Exception as e:  # noqa: BLE001 — a demo must never break the pipeline
        return {"store": f"feast (skipped: {e})"}
```

Merge it into the results `feature_store` block:

```python
        "feature_store": {
            "n_patients": int(len(feats)),
            "features": [c for c in feats.columns if c not in ("patient_key", "surgery_90d", "event_timestamp")],
            "offline_table": "gold.patient_features",
            "parquet": "data/gold/patient_features.parquet",
            **feast_demo,
        },
```

- [ ] **Step 3: Verify `ml/feature_store/features.py` matches the parquet fields**

Confirm the FeatureView's 8 `Field` names/dtypes correspond to real parquet columns (`age`→Int64, the six `Float32`, `n_observations`→Int64). Run:
```bash
rg -n "Field\(name=" ml/feature_store/features.py
```
Expected: exactly the 8 in `FEATURES`. **Only if** a name/type is wrong, fix that line; otherwise leave the file unchanged.

- [ ] **Step 4: Add the Makefile target + gitignore the Feast artifacts**

`Makefile` (after the `rag-query` block; add the names to `.PHONY`):
```make
feast-demo:     ## apply + materialize Feast (offline parquet -> sqlite online) + online/historical retrieval
	PYTHONPATH=src ./.venv/bin/python -m vitals.feature_store demo
```

`.gitignore` (append):
```
# Feast local store artifacts (registry + online sqlite, generated by apply/materialize)
ml/feature_store/registry.db
ml/feature_store/online_store.db
ml/feature_store/data/
```

- [ ] **Step 5: Install the extra and verify the demo end-to-end (parity all-match)**

```bash
uv sync --extra dev --extra local --extra feast
make run                      # builds gold + writes the timestamped parquet + runs _feature_store_demo
uv run --extra local --extra feast python -c "import json; r=json.load(open('data/results.json')); \
print('online:', r['feature_store']['online_parity']['all_match']); \
print('historical:', r['feature_store']['historical_parity']['all_match'])"
```
Expected: both print `True` — online + point-in-time historical retrieval reproduce the offline parquet. Also run `make feast-demo` and confirm it prints a JSON blob with `all_match: true` for both. If a parity is `False`, STOP and report which feature/patient diverged (likely a TTL/timestamp window issue or a dtype mismatch) — do not proceed.

- [ ] **Step 6: Confirm no Feast artifacts staged + hermetic suite still green**

```bash
git status --porcelain | rg "registry.db|online_store.db|feast_repo|/data/" && echo "!! artifact leaked" || echo "clean"
uv run --extra dev --extra local pytest tests/ -q      # no feast extra: feast tests still pass (pure) / skip
```
Expected: `clean`; suite passes (Task 1's pure tests pass without feast).

- [ ] **Step 7: Commit**

```bash
git add src/vitals/feature_store.py src/vitals/serve.py Makefile .gitignore
git add ml/feature_store/features.py 2>/dev/null || true
git commit -m "feat(feast): materialize + online/historical retrieval; wire into serve

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01F1hdqzju3WbgEALYreWUjk"
```

---

### Task 3: gated parity test + ADR 0008 + README

Lock the online+historical == offline contract in a gated test, and document the store.

**Files:**
- Modify: `tests/test_feature_store.py` (append the gated integration test)
- Create: `docs/adr/0008-feast-feature-store.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: `feature_store.demo` / `is_available` (Task 2); the offline parquet from `make run`.

- [ ] **Step 1: Append the gated parity test**

Add to `tests/test_feature_store.py`:

```python
pytest.importorskip("feast")

from pathlib import Path  # noqa: E402

_PARQUET = Path(__file__).resolve().parents[1] / "data" / "gold" / "patient_features.parquet"


@pytest.mark.skipif(not _PARQUET.exists(), reason="needs `make run` to write the feature parquet")
def test_feast_online_and_historical_match_offline():
    """Real Feast retrieval must reproduce the offline parquet (online + point-in-time historical).
    Gated: skips in CI (no `feast` extra) and when the parquet isn't built."""
    result = fs.demo(n=5)
    assert result["online_parity"]["all_match"] is True, result["online_parity"]
    assert result["historical_parity"]["all_match"] is True, result["historical_parity"]
```

Note: `pytest.importorskip("feast")` sits at module level *below* the Task-1 pure tests — it must be placed so it only gates the integration test. Put the Task-1 pure tests (which need no Feast) ABOVE this line; everything below is Feast-gated. (In CI, the module still imports and the pure tests run; `importorskip` raises Skipped only for what follows — so keep the pure tests first.)

Correction for a clean gate: put the pure tests in their own module-top section, and guard only the integration test with a local skip instead of a module-level importorskip, to avoid skipping the pure tests:

```python
@pytest.mark.skipif(
    __import__("importlib.util", fromlist=["find_spec"]).find_spec("feast") is None
    or not _PARQUET.exists(),
    reason="needs the `feast` extra + `make run` (gated; skips in CI)",
)
def test_feast_online_and_historical_match_offline():
    result = fs.demo(n=5)
    assert result["online_parity"]["all_match"] is True, result["online_parity"]
    assert result["historical_parity"]["all_match"] is True, result["historical_parity"]
```
Use this local-skip form (not module-level `importorskip`) so the Task-1 pure tests always run in CI. Remove the `pytest.importorskip("feast")` line if you added it.

- [ ] **Step 2: Run the gated test both ways**

```bash
uv run --extra dev --extra local --extra feast pytest tests/test_feature_store.py -q   # with feast + parquet
uv run --extra dev --extra local pytest tests/test_feature_store.py -q                 # no feast: integration SKIPS, pure PASS
```
Expected: first run — all pass (4 pure + 1 integration); second run — 4 pass, 1 skipped.

- [ ] **Step 3: Write ADR 0008**

Create `docs/adr/0008-feast-feature-store.md`:

```markdown
# ADR 0008 — Feast feature store (the third gold store)

**Status:** accepted · 2026-06-30

## Context
The gold layer serves three shapes: analytics marts (dbt), a vector index (pgvector), and an ML
**feature store**. The feature store was scaffolded (`ml/feature_store/` — an entity + an 8-field
FeatureView + a local sqlite/file config) but never applied, materialized, or retrieved, so the
"three-store gold" claim had a hole.

## Decision
Make it real with **Feast**, local: sqlite online store + file offline store, sourced from the gold
feature parquet the pipeline already produces. Demonstrate the two things a feature store exists for:
- **Online retrieval** (`get_online_features`) — low-latency features for inference.
- **Point-in-time historical retrieval** (`get_historical_features`) — a leakage-safe training join
  over an entity dataframe (the same no-leakage discipline as the `surgery_90d` label).

Both are **parity-checked** against the offline parquet — the store must return the values the pipeline
produced (NULL-aware, float-tolerant).

Key choices:
- **Optional `feast` extra + graceful skip.** `serve.py` runs the demo only when Feast is installed
  (like the pgvector store); `make build` (`--no-serve`) never touches it, so clone-and-run and the
  hermetic CI gate are unchanged. The integration test skips in CI.
- **Deterministic, TTL-safe timestamps.** A fixed `event_timestamp` (2026-01-01), materialize end, and
  query time all sit inside the FeatureView's 90-day ttl window; tz-aware UTC throughout. Otherwise
  retrieval silently returns null.
- **The model keeps training on the parquet.** The historical demo proves PIT retrieval is correct;
  rewiring the MLflow model through Feast is out of scope.

## Consequences
- New runnable path: `make feast-demo` (+ the demo runs inside `make run` when the extra is present);
  `results.json` gains `feature_store.online_parity` / `historical_parity`.
- Registry + online sqlite are gitignored build artifacts.
- Production would point the offline store at Databricks/Delta — noted, not exercised (local is the
  deliverable, same stance as MetricFlow in ADR 0007).

## Alternatives considered
- **Leave it as a parquet + a table:** no online serving, no point-in-time joins — a feature *file*,
  not a feature *store*; the three-store claim stays half-true.
- **Online retrieval only:** simpler, but omits the point-in-time training join, which is the deeper
  ML-correctness (no-leakage) story.
- **Feast on Databricks/Delta now:** out of scope for a local, reproducible showcase.
```

- [ ] **Step 4: Update the README feature-store row**

In `README.md`, the three-store table row:
```markdown
| **Feature store** | Feast (offline + online) | surgery-risk / adherence ML models |
```
becomes:
```markdown
| **Feature store** | Feast (sqlite online + file offline) | surgery-risk features, online + point-in-time (`make feast-demo`) |
```

- [ ] **Step 5: Commit**

```bash
git add tests/test_feature_store.py docs/adr/0008-feast-feature-store.md README.md
git commit -m "test+docs(feast): gated online/historical parity test + ADR 0008 + README

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01F1hdqzju3WbgEALYreWUjk"
```

---

## Self-Review

**Spec coverage:**
- Pure helpers (entity_df, NULL-aware parity) → Task 1. ✓
- Lazy Feast I/O (apply/materialize/online/historical/demo/main) → Task 2 Step 1. ✓
- `event_timestamp` on parquet (deterministic, tz-aware, TTL-safe) → Task 2 Step 2 + constants Task 1. ✓
- serve `_feature_store_demo` pgvector-style skip → Task 2 Step 2. ✓
- FeatureView field verification → Task 2 Step 3. ✓
- Makefile + gitignore artifacts → Task 2 Step 4. ✓
- End-to-end parity (online + historical == offline) → Task 2 Step 5 + Task 3 Step 1. ✓
- Gated test skips in CI / hermetic pure tests run → Task 3 Steps 1-2. ✓
- ADR 0008 + README + results.json note → Task 3 Steps 3-4 (results via Task 2 Step 2). ✓
- Non-goals (model on parquet, local only) → no task violates them. ✓

**Placeholder scan:** none. The Task 3 Step 1 note gives the exact final skip form (local `skipif` with `find_spec`) to use — not a vague instruction; the earlier `importorskip` variant is explicitly corrected to avoid skipping the pure tests.

**Type consistency:** `FEATURES`/`REFS`/`entity_df`/`parity` defined in Task 1 are consumed with the same signatures in Task 2 (`online_features`/`historical_features` return DataFrames → `parity(retrieved_df, offline_df, keys)`). `demo()` return keys (`online_parity`/`historical_parity`/`all_match`) match the serve merge (Task 2 Step 2) and the test assertions (Task 3 Step 1). `EVENT_TS`=2026-01-01 matches the serve stamp; `MATERIALIZE_END`/`QUERY_TS`=2026-03-01 within the 90-day ttl of EVENT_TS.
```
