"""Parity test — the dbt Semantic Layer (MetricFlow) must reproduce the mart numbers.

Gated like the pgvector integration test: needs the `metrics` extra AND the dbt-built DuckDB
warehouse, so it SKIPS in CI (which installs only --extra dev and never builds the warehouse before
pytest). Run locally: `uv sync --extra dev --extra metrics && make build && \
uv run --extra dev --extra metrics pytest tests/test_semantic_layer.py -q`.
"""
import csv
import os
import subprocess
from pathlib import Path

import pytest

pytest.importorskip("metricflow")
import duckdb  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "vitals.duckdb"
DBT = ROOT / "dbt"
MF = ROOT / ".venv" / "bin" / "mf"

pytestmark = pytest.mark.skipif(
    not DB.exists() or not MF.exists(),
    reason="needs `make build` + `uv sync --extra metrics`",
)

# metric -> (mart table, mart column, decimals the mart rounds to)
CASES = {
    "surgery_rate": ("mart_condition_outcomes", "surgery_rate", 3),
    "avg_pain": ("mart_condition_outcomes", "avg_pain", 2),
    "avg_conservative_spend": ("mart_cost_outcomes", "avg_conservative_spend", 0),
}


def _mf_query(metric: str, out: Path) -> dict[str, float]:
    subprocess.run(
        [str(MF), "query", "--metrics", metric,
         "--group-by", "patient__primary_condition", "--csv", str(out)],
        cwd=DBT, env={**os.environ, "DBT_PROFILES_DIR": "."}, check=True,
    )
    rows = {}
    with out.open() as fh:
        for r in csv.DictReader(fh):
            cond = r.get("patient__primary_condition") or r.get("primary_condition")
            val = r.get(metric)
            if cond and val not in (None, "", "None"):
                rows[cond] = float(val)
    return rows


def _mart_values(table: str, col: str) -> dict[str, float]:
    con = duckdb.connect(str(DB))
    df = con.execute(
        f"select primary_condition, {col} from gold.{table} where primary_condition is not null"
    ).df()
    con.close()
    return {row.primary_condition: float(getattr(row, col)) for row in df.itertuples()}


@pytest.mark.parametrize("metric", list(CASES))
def test_metric_matches_mart(metric, tmp_path):
    table, col, decimals = CASES[metric]
    sl = _mf_query(metric, tmp_path / f"{metric}.csv")
    mart = _mart_values(table, col)
    assert mart, f"{table}.{col} returned no rows — mart not built? run `make build`"
    assert set(sl) == set(mart), f"{metric}: condition groups differ"
    for cond, mart_val in mart.items():
        assert round(sl[cond], decimals) == mart_val, (
            f"{metric}[{cond}]: SL {sl[cond]} (round {decimals}) != mart {mart_val}"
        )
