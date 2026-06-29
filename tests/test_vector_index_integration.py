"""Integration tests for the pgvector store — Docker-gated, SKIPPED in CI (no Docker, no extra).

Run locally: `uv sync --extra dev --extra vector && make rag-up && uv run --extra dev --extra vector \
pytest tests/test_vector_index_integration.py -q`.
"""
import pandas as pd
import pytest

pytest.importorskip("psycopg")
pytest.importorskip("fastembed")

from vitals import vector_index as vx  # noqa: E402

pytestmark = pytest.mark.skipif(not vx.is_available(), reason="pgvector not reachable (make rag-up)")


def _fixture() -> pd.DataFrame:
    return pd.DataFrame({
        "patient_key": ["p1", "p2", "p3"],
        "text": [
            "Patient reports severe lower back pain worse when sitting, poor medication adherence.",
            "Shoulder pain with overhead reaching, started after the gym.",
            "Routine diabetes follow-up, glucose stable, no acute complaints.",
        ],
    })


@pytest.fixture(autouse=True)
def _clean_table():
    conn = vx.connect()
    conn.execute(f"DROP TABLE IF EXISTS {vx.TABLE}")
    conn.commit()
    conn.close()
    yield


def test_load_then_query_ranks_relevant_note_first():
    assert vx.load_notes(_fixture()) == 3
    matches = vx.query("low back pain when sitting", k=3)
    assert matches[0]["note"].lower().startswith("patient reports severe lower back pain")


def test_reload_is_idempotent():
    df = _fixture()
    assert vx.load_notes(df) == 3
    assert vx.load_notes(df) == 3  # upsert on note_id — no duplication
