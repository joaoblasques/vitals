"""Vector index — clinical-note embeddings in Postgres + pgvector (the real gold vector store).

A local serving store standing in for a managed Postgres in prod (the vitals_gold.vectors UC schema
is the conceptual home). Dense semantic embeddings (fastembed bge-small-en-v1.5, 384-d) of
silver.note text, indexed with an HNSW cosine index, queried by ANN. Retrieval-only.

Pure SQL/id helpers are separated from I/O so the contract is unit-testable without a database; the
psycopg/pgvector/fastembed imports are lazy so a clone without the `vector` extra still imports this
module (serve.py falls back to TF-IDF when the store or the deps are absent).

Run: `make rag-up`, then `python -m vitals.vector_index load` / `... query "low back pain"`.
"""
from __future__ import annotations

import hashlib
import os
import sys

MODEL = "BAAI/bge-small-en-v1.5"
DIM = 384
TABLE = "note_embeddings"
TOPK = 3


# ---- pure logic (no DB, no model — unit-testable) ---------------------------------------------

def note_id(patient_key: str, text: str) -> str:
    """Deterministic upsert key for a note — stable across reloads (idempotency)."""
    return hashlib.md5(f"{patient_key}\n{text}".encode()).hexdigest()


def ddl() -> list[str]:
    """Extension + table + HNSW cosine index — all idempotent (IF NOT EXISTS)."""
    return [
        "CREATE EXTENSION IF NOT EXISTS vector",
        f"CREATE TABLE IF NOT EXISTS {TABLE} ("
        f"note_id text PRIMARY KEY, patient_key text, text text, embedding vector({DIM}))",
        f"CREATE INDEX IF NOT EXISTS {TABLE}_embedding_hnsw "
        f"ON {TABLE} USING hnsw (embedding vector_cosine_ops)",
    ]


def upsert_sql() -> str:
    return (
        f"INSERT INTO {TABLE} (note_id, patient_key, text, embedding) VALUES (%s, %s, %s, %s) "
        "ON CONFLICT (note_id) DO UPDATE SET embedding = EXCLUDED.embedding, text = EXCLUDED.text"
    )


def query_sql(k: int = TOPK) -> str:
    # <=> is pgvector cosine distance; 1 - distance = cosine similarity.
    return (
        f"SELECT patient_key, text, 1 - (embedding <=> %s) AS score "
        f"FROM {TABLE} ORDER BY embedding <=> %s LIMIT {int(k)}"
    )


def shape_matches(rows: list[tuple]) -> list[dict]:
    """Map (patient_key, text, score) rows -> the serving result shape."""
    return [
        {"patient_key": pk, "score": round(float(score), 3), "note": text[:160]}
        for pk, text, score in rows
    ]


# ---- I/O (lazy imports: fastembed / psycopg / pgvector) ---------------------------------------

_EMBEDDER = None


def _embedder():
    global _EMBEDDER
    if _EMBEDDER is None:
        from fastembed import TextEmbedding
        _EMBEDDER = TextEmbedding(model_name=MODEL)
    return _EMBEDDER


def embed_documents(texts):
    """Document embeddings (numpy arrays, 384-d) for the given texts."""
    return list(_embedder().embed(list(texts)))


def embed_query(text: str):
    """Query embedding (BGE uses a query prefix via query_embed)."""
    return list(_embedder().query_embed(text))[0]


def _conn_kwargs() -> dict:
    return {
        "host": os.getenv("PGHOST", "localhost"),
        "port": int(os.getenv("PGPORT", "5432")),
        "user": os.getenv("PGUSER", "vitals"),
        "password": os.getenv("PGPASSWORD", "vitals"),
        "dbname": os.getenv("PGDATABASE", "vitals"),
    }


def connect():
    """psycopg 3 connection with the pgvector type adapter registered."""
    import psycopg
    from pgvector.psycopg import register_vector

    conn = psycopg.connect(**_conn_kwargs())
    conn.execute("CREATE EXTENSION IF NOT EXISTS vector")  # adapter needs the type to exist
    conn.commit()
    register_vector(conn)
    return conn


def is_available() -> bool:
    """True if a Postgres is reachable (drives serve.py's fallback). Fails fast when Docker is down."""
    try:
        import psycopg
    except ModuleNotFoundError:
        return False
    try:
        with psycopg.connect(**_conn_kwargs(), connect_timeout=3) as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:
        return False


def load_notes(df) -> int:
    """Embed + upsert notes into pgvector; return the table row count. Idempotent (note_id PK)."""
    df = df.drop_duplicates(subset="text").reset_index(drop=True)
    texts, keys = df["text"].tolist(), df["patient_key"].tolist()
    vectors = embed_documents(texts)
    rows = [(note_id(k, t), k, t, v) for k, t, v in zip(keys, texts, vectors)]
    conn = connect()
    try:
        for stmt in ddl():
            conn.execute(stmt)
        conn.commit()
        with conn.cursor() as cur:
            cur.executemany(upsert_sql(), rows)
        conn.commit()
        n = conn.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]
    finally:
        conn.close()
    return int(n)


def query(text: str, k: int = TOPK) -> list[dict]:
    """Embed the query and return the top-k nearest notes by cosine similarity."""
    q = embed_query(text)
    conn = connect()
    try:
        rows = conn.execute(query_sql(k), (q, q)).fetchall()
    finally:
        conn.close()
    return shape_matches(rows)


def rag_demo(notes_df, queries: list[str]) -> dict:
    """serve.py entrypoint: load notes, run the demo queries, return the serving result shape."""
    n = load_notes(notes_df)
    return {
        "n_notes_indexed": n,
        "embedding": f"{MODEL} (pgvector, cosine)",
        "dim": DIM,
        "demo_queries": [{"query": q, "matches": query(q)} for q in queries],
    }


def main(argv: list[str] | None = None) -> None:
    import json

    argv = sys.argv[1:] if argv is None else argv
    cmd = argv[0] if argv else "load"
    if cmd == "load":
        import duckdb
        from pathlib import Path
        db = Path(__file__).resolve().parents[2] / "data" / "vitals.duckdb"
        with duckdb.connect(str(db)) as con:
            notes = con.execute("SELECT patient_key, text FROM silver.note").df()
        print(f"loaded {load_notes(notes)} note embeddings into {TABLE}")
    elif cmd == "query":
        q = argv[1] if len(argv) > 1 else "low back pain worse with sitting"
        print(json.dumps({"query": q, "matches": query(q)}, indent=2))
    else:
        raise SystemExit('usage: python -m vitals.vector_index load | query "<text>"')


if __name__ == "__main__":
    main()
