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
