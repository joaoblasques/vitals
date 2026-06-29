"""Unit tests for the pgvector store's pure contract (no DB, no model — hermetic, runs in CI)."""
from vitals import vector_index as vx


def test_note_id_deterministic_and_unique():
    a = vx.note_id("p1", "low back pain")
    assert a == vx.note_id("p1", "low back pain")          # stable across reloads
    assert a != vx.note_id("p1", "shoulder pain")           # text-sensitive
    assert a != vx.note_id("p2", "low back pain")           # patient-sensitive


def test_ddl_creates_extension_table_and_hnsw_cosine_index():
    stmts = " ".join(vx.ddl())
    assert "CREATE EXTENSION IF NOT EXISTS vector" in stmts
    assert "vector(384)" in stmts
    assert "USING hnsw (embedding vector_cosine_ops)" in stmts


def test_upsert_sql_is_idempotent_on_note_id():
    assert "ON CONFLICT (note_id) DO UPDATE" in vx.upsert_sql()


def test_query_sql_uses_cosine_distance_and_limit():
    sql = vx.query_sql(5)
    assert "1 - (embedding <=> %s) AS score" in sql
    assert "<=>" in sql and "LIMIT 5" in sql


def test_shape_matches_rounds_score_and_truncates_note():
    long = "x" * 200
    out = vx.shape_matches([("p1", long, 0.912345)])
    assert out == [{"patient_key": "p1", "score": 0.912, "note": "x" * 160}]


def test_serve_rag_routes_to_pgvector_when_available(monkeypatch):
    """When pgvector is reachable, serve._rag_demo delegates to vector_index.rag_demo."""
    import pandas as pd
    from vitals import serve, vector_index
    monkeypatch.setattr(vector_index, "is_available", lambda: True)
    monkeypatch.setattr(vector_index, "rag_demo", lambda notes, queries: {"routed": "pgvector"})
    notes = pd.DataFrame({"patient_key": ["a"], "text": ["low back pain"]})
    assert serve._rag_demo(notes, ["q"]) == {"routed": "pgvector"}


def test_serve_rag_falls_back_to_tfidf_when_unavailable(monkeypatch):
    """When pgvector is down, serve._rag_demo uses TF-IDF (and does not touch psycopg/fastembed)."""
    import pandas as pd
    from vitals import serve, vector_index
    monkeypatch.setattr(vector_index, "is_available", lambda: False)
    notes = pd.DataFrame({
        "patient_key": ["a", "b", "c", "d"],
        "text": ["low back pain worse sitting", "low back pain improving",
                 "shoulder pain overhead", "shoulder pain reaching"],
    })
    res = serve._rag_demo(notes, ["low back pain"])
    assert res["embedding"].startswith("TF-IDF")
    assert res["n_notes_indexed"] == 4
