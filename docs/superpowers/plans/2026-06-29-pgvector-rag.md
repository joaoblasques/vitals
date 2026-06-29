# pgvector Vector Index Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the in-memory TF-IDF placeholder in `serve.py` with a real pgvector store — fastembed `bge-small-en-v1.5` (384-d) embeddings of clinical notes, HNSW cosine ANN in Dockerized Postgres+pgvector — wired into the serving layer with a graceful TF-IDF fallback.

**Architecture:** A new `src/vitals/vector_index.py` owns the store (pure SQL/id helpers split from psycopg/fastembed I/O). `serve._rag_demo` uses it when pgvector is reachable, else falls back to the existing TF-IDF path, so clone-and-run and hermetic CI never require Docker. A committed `docker-compose.yml` stands up pgvector; `make rag-*` targets drive it.

**Tech Stack:** Postgres + pgvector (`pgvector/pgvector:pg16`), `pgvector` + `psycopg[binary]` (psycopg 3), `fastembed` (ONNX, `BAAI/bge-small-en-v1.5`), DuckDB (source notes), pytest.

## Global Constraints

- Embedding model **`BAAI/bge-small-en-v1.5`**, dimension **384** (`vector(384)`), cosine distance (`<=>`), HNSW index (`vector_cosine_ops`).
- pgvector deps live in a **new optional extra `vector`** — NOT core. Imports of psycopg/pgvector/fastembed are **lazy**; module must import without the extra installed.
- **Clone-and-run / hermetic CI unaffected:** `make run` and `make build` must complete with Docker down and the extra absent (TF-IDF fallback). Unit tests are hermetic; the integration test is Docker-gated and skips otherwise.
- **Idempotent:** `note_id = md5(patient_key\ntext)` PK + `ON CONFLICT DO UPDATE`; reload keeps row count stable.
- **No secrets:** local-dev Postgres creds `vitals/vitals/vitals` on localhost, overridable via libpq env vars (`PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE`).
- Retrieval-only — no LLM/generation.
- Verified library APIs: `from pgvector.psycopg import register_vector`; `TextEmbedding(model_name=...).embed(docs)` / `.query_embed(q)` return numpy arrays.

---

### Task 1: `vector_index` pure core (id + SQL + shaping)

The hermetic, dependency-free contract of the store: the upsert key, the DDL/SQL strings, and result shaping. No DB, no model — fully unit-testable and runs in CI.

**Files:**
- Create: `src/vitals/vector_index.py` (module header + constants + pure functions)
- Test: `tests/test_vector_index.py`

**Interfaces:**
- Produces: `MODEL="BAAI/bge-small-en-v1.5"`, `DIM=384`, `TABLE="note_embeddings"`, `TOPK=3`.
- Produces: `note_id(patient_key:str, text:str)->str`; `ddl()->list[str]`; `upsert_sql()->str`; `query_sql(k:int=TOPK)->str`; `shape_matches(rows:list[tuple])->list[dict]` (`{patient_key, score, note}`, score rounded 3 dp, note truncated 160).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_vector_index.py`:

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --extra dev pytest tests/test_vector_index.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'vitals.vector_index'`.

- [ ] **Step 3: Create the module with constants + pure functions**

Create `src/vitals/vector_index.py`:

```python
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --extra dev pytest tests/test_vector_index.py -q`
Expected: PASS (5 passed).

- [ ] **Step 5: Lint**

Run: `uv run ruff check src/vitals/vector_index.py tests/test_vector_index.py`
Expected: `All checks passed!` (the module imports only `hashlib` here; `os`/`sys` are added with their uses in Task 2.)

- [ ] **Step 6: Commit**

```bash
git add src/vitals/vector_index.py tests/test_vector_index.py
git commit -m "feat(vector): pgvector store pure core (note_id + SQL + shaping)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01F1hdqzju3WbgEALYreWUjk"
```

---

### Task 2: `vector_index` I/O + deps + compose + Makefile + integration test

Add the live machinery — embeddings, connection, load/query, CLI — plus the `vector` extra, the Docker compose service, the `make rag-*` targets, and a Docker-gated integration test. Deliverable: `make rag-up` + `python -m vitals.vector_index load`/`query` works end-to-end, and the integration test passes with pgvector up.

**Files:**
- Modify: `src/vitals/vector_index.py` (append I/O layer + `main()`)
- Modify: `pyproject.toml` (add `vector` extra after `feast`, line 25)
- Create: `docker-compose.yml`
- Modify: `Makefile` (`.PHONY` + `rag-up`/`rag-down`/`rag-load`/`rag-query`)
- Test: `tests/test_vector_index_integration.py`

**Interfaces:**
- Consumes (Task 1): `MODEL`, `DIM`, `TABLE`, `TOPK`, `note_id`, `ddl`, `upsert_sql`, `query_sql`, `shape_matches`.
- Produces: `embed_documents(texts)->list[np.ndarray]`; `embed_query(text)->np.ndarray`; `connect()`; `is_available()->bool`; `load_notes(df)->int`; `query(text, k=TOPK)->list[dict]`; `rag_demo(notes_df, queries)->dict` (`{n_notes_indexed, embedding, dim, demo_queries}`); `main(argv=None)`.

- [ ] **Step 1: Add the `vector` optional extra**

In `pyproject.toml`, after the `feast = ["feast>=0.40"]` line (line 25), add:

```toml
# Vector index serving store — local pgvector (Docker) + fastembed embeddings (ADR 0006).
vector = ["fastembed>=0.3", "psycopg[binary]>=3.1", "pgvector>=0.3"]
```

- [ ] **Step 2: Create `docker-compose.yml`**

```yaml
# Local pgvector serving store — the gold vector index (ADR 0006). `make rag-up` to start.
# Dev-only credentials, NOT secrets; override via PG* env vars. Ephemeral (reloads are idempotent).
services:
  pgvector:
    image: pgvector/pgvector:pg16
    environment:
      POSTGRES_USER: vitals
      POSTGRES_PASSWORD: vitals
      POSTGRES_DB: vitals
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U vitals -d vitals"]
      interval: 3s
      timeout: 3s
      retries: 10
```

- [ ] **Step 3: Append the I/O layer + `main()` to `src/vitals/vector_index.py`**

First add `import os` and `import sys` to the top-of-file import block (alphabetical, alongside `import hashlib`). Then append after the pure functions:

```python
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
        notes = duckdb.connect(str(db)).execute("SELECT patient_key, text FROM silver.note").df()
        print(f"loaded {load_notes(notes)} note embeddings into {TABLE}")
    elif cmd == "query":
        q = argv[1] if len(argv) > 1 else "low back pain worse with sitting"
        print(json.dumps({"query": q, "matches": query(q)}, indent=2))
    else:
        raise SystemExit('usage: python -m vitals.vector_index load | query "<text>"')


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Add the `make rag-*` targets**

In `Makefile`, add the four targets to `.PHONY` (after `drift-databricks`) and append the targets after the `drift-databricks` block:

```make
rag-up:         ## start the local pgvector serving store (Docker) + wait until healthy
	docker compose up -d pgvector
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' $$(docker compose ps -q pgvector))" = "healthy" ]; do sleep 1; done
	@echo "pgvector healthy on localhost:5432"

rag-down:       ## stop + remove the pgvector store
	docker compose down

rag-load:       ## embed silver.note -> pgvector (needs `uv sync --extra vector` + rag-up)
	PYTHONPATH=src ./.venv/bin/python -m vitals.vector_index load

rag-query:      ## ANN query the store: make rag-query Q="low back pain"
	PYTHONPATH=src ./.venv/bin/python -m vitals.vector_index query "$(Q)"
```

- [ ] **Step 5: Install the extra and stand up pgvector**

Run:
```bash
uv sync --extra dev --extra vector
make rag-up
```
Expected: deps resolve (fastembed, psycopg, pgvector); `pgvector healthy on localhost:5432`.

- [ ] **Step 6: Write the Docker-gated integration test**

Create `tests/test_vector_index_integration.py`:

```python
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
```

- [ ] **Step 7: Run the integration test (Docker up)**

Run: `uv run --extra dev --extra vector pytest tests/test_vector_index_integration.py -q`
Expected: PASS (2 passed). First run downloads the bge-small ONNX model (~67 MB) once. If it fails, inspect with `make rag-query Q="low back pain when sitting"`.

- [ ] **Step 8: Verify hermetic suite still green (no Docker needed for it)**

Run: `uv run --extra dev pytest tests/test_vector_index.py -q && uv run ruff check src/vitals/vector_index.py`
Expected: 5 passed; `All checks passed!`.

- [ ] **Step 9: Commit**

```bash
git add src/vitals/vector_index.py pyproject.toml docker-compose.yml Makefile tests/test_vector_index_integration.py uv.lock
git commit -m "feat(vector): pgvector I/O + docker-compose + make rag-* + integration test

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01F1hdqzju3WbgEALYreWUjk"
```

---

### Task 3: Wire serve.py fallback + docs

Make the serving layer prefer pgvector with a TF-IDF fallback, prove the fallback hermetically, and document the decision (ADR 0006 + README).

**Files:**
- Modify: `src/vitals/serve.py` (`_rag_demo`, line 108; docstring line 4)
- Modify: `tests/test_vector_index.py` (append the fallback test)
- Create: `docs/adr/0006-pgvector-local-serving-store.md`
- Modify: `README.md` (line 35 table row)

**Interfaces:**
- Consumes: `vitals.vector_index.is_available()`, `vitals.vector_index.rag_demo(notes, queries)`.

- [ ] **Step 1: Write the failing routing tests**

Append to `tests/test_vector_index.py` (two tests — the *routing-to-pgvector* one fails against current code, which ignores `vector_index`; the *fallback* one pins the no-Docker path):

```python
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
```

- [ ] **Step 2: Run them to verify the routing test fails**

Run: `uv run --extra dev pytest tests/test_vector_index.py -k "routes_to_pgvector or falls_back" -q`
Expected: `test_serve_rag_routes_to_pgvector_when_available` FAILS (current `serve._rag_demo` ignores `vector_index` and runs TF-IDF, returning a dict with `embedding`/`n_notes_indexed`, not `{"routed": "pgvector"}`). The fallback test passes already (TF-IDF is the current behavior) — that's expected.

- [ ] **Step 3: Wire pgvector-with-fallback into `serve._rag_demo`**

In `src/vitals/serve.py`, replace the body opening of `_rag_demo` (currently lines 108-114, from `def _rag_demo` through the `mat = vec.fit_transform(...)` line) so it first tries pgvector. Replace:

```python
def _rag_demo(notes: pd.DataFrame, queries: list[str]) -> dict:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity

    notes = notes.drop_duplicates(subset="text").reset_index(drop=True)
    vec = TfidfVectorizer(stop_words="english", min_df=2)
    mat = vec.fit_transform(notes["text"])
```

with:

```python
def _rag_demo(notes: pd.DataFrame, queries: list[str]) -> dict:
    # Prefer the real pgvector store; fall back to in-memory TF-IDF when it (or the `vector` extra)
    # is unavailable, so clone-and-run / hermetic CI never depend on Docker.
    try:
        from vitals import vector_index as vx
        if vx.is_available():
            return vx.rag_demo(notes, queries)
    except Exception:
        pass

    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity

    notes = notes.drop_duplicates(subset="text").reset_index(drop=True)
    vec = TfidfVectorizer(stop_words="english", min_df=2)
    mat = vec.fit_transform(notes["text"])
```

And update the fallback's `embedding` label (currently `"TF-IDF (prod target: pgvector + clinical embeddings)"`, line ~125) to:

```python
        "embedding": "TF-IDF (fallback; prod target: pgvector + clinical embeddings)",
```

- [ ] **Step 4: Update the `serve.py` module docstring (line 4)**

Change line 4 from:

```python
2. Vector index   : TF-IDF embeddings of clinical notes + a cosine RAG query (pgvector in prod).
```

to:

```python
2. Vector index   : clinical-note embeddings + cosine RAG — real pgvector when up (vitals.vector_index),
                    in-memory TF-IDF fallback otherwise.
```

- [ ] **Step 5: Run the fallback test + full hermetic suite**

Run: `uv run --extra dev pytest tests/ -q`
Expected: PASS — green suite, **29 passed, 2 skipped** (22 prior + 5 `test_vector_index.py` core + 2 routing/fallback; the 2 Docker-gated integration tests SKIP without pgvector/the extra).

- [ ] **Step 6: Write ADR 0006**

Create `docs/adr/0006-pgvector-local-serving-store.md`:

```markdown
# ADR 0006 — pgvector as a local serving store (real vector index), TF-IDF as fallback

**Status:** accepted · 2026-06-29

## Context
The gold layer promises three serving stores; the vector index was a TF-IDF + in-memory cosine
placeholder in `serve.py` (tagged "prod target: pgvector"). Unity Catalog has no pgvector — the
`vitals_gold.vectors` schema is a conceptual placeholder. A real vector index needs a vector
database. We want to demonstrate genuine semantic retrieval without breaking the clone-and-run
default or the hermetic, no-creds CI gate.

## Decision
Run **pgvector locally in Docker** (`pgvector/pgvector:pg16`) as the real vector store, with dense
embeddings from **fastembed `bge-small-en-v1.5`** (384-d, ONNX/CPU, no torch, no API keys). Wire it
into `serve.py` behind `vitals.vector_index.is_available()`, falling back to the existing TF-IDF
path when pgvector or the optional `vector` extra is absent. Retrieval-only (no LLM generation —
the RAG demo proves the data is AI-ready, it is not an LLM project).

Rationale:
- **Real, but reproducible from code** — a committed `docker-compose.yml` + `make rag-*` targets;
  no manual setup. HNSW cosine index, idempotent upsert on a deterministic `note_id`.
- **Clone-and-run + hermetic CI preserved** — pgvector is an opt-in extra; `make run`/`make build`
  complete with Docker down via the TF-IDF fallback. Unit tests are hermetic; the integration test
  is Docker-gated and skips in CI.
- **Honest prod story** — locally Dockerized pgvector stands in for a managed Postgres+pgvector in
  prod; `vitals_gold.vectors` documents that conceptual home.

## Consequences
- New optional dependency group `vector` (`fastembed`, `psycopg[binary]`, `pgvector`); not core.
- The store is local-only; embeddings are not computed on Databricks (out of scope).
- First load downloads the bge-small ONNX model (~67 MB), cached thereafter.
- Dev-only Postgres creds (`vitals/vitals/vitals`, localhost) — not secrets; override via PG* env.

## Alternatives considered
- **sentence-transformers** embeddings: same 384-d result but pulls in torch/transformers — heavier
  for no benefit here.
- **Store TF-IDF vectors in pgvector**: lexical not semantic, and high-dim sparse vectors fit
  pgvector awkwardly.
- **Managed cloud vector DB / Databricks Vector Search**: not Free-Edition-available and overkill
  for a local showcase; the Docker pgvector path is the reproducible, inspectable choice.
```

- [ ] **Step 7: Update the README table row (line 35)**

Change:

```markdown
| **Vector index** | pgvector | RAG / semantic search over clinical notes |
```

to:

```markdown
| **Vector index** | pgvector | RAG / semantic search over clinical notes (`make rag-up` for the real store; TF-IDF fallback otherwise) |
```

- [ ] **Step 8: End-to-end verification — real pgvector path via `make run`**

With pgvector up (`make rag-up`) and the extra installed, run:
```bash
uv run --extra dev --extra vector python -m vitals.run --no-serve   # rebuild data so silver.note exists
make rag-load
make rag-query Q="severe lower back pain worse with sitting, poor adherence"
```
Expected: `rag-load` prints `loaded <N> note embeddings into note_embeddings`; `rag-query` returns JSON with a clinically relevant note ranked first (score near 1.0). This exercises the real store the way `serve.run()` will when pgvector is up.

- [ ] **Step 9: Commit**

```bash
git add src/vitals/serve.py tests/test_vector_index.py docs/adr/0006-pgvector-local-serving-store.md README.md
git commit -m "feat(serve): pgvector RAG with TF-IDF fallback + ADR 0006

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01F1hdqzju3WbgEALYreWUjk"
```

---

## Self-Review

**Spec coverage:**
- pgvector store module (pure + I/O) → Tasks 1, 2. ✓
- fastembed bge-small-en-v1.5 384-d, HNSW cosine, `<=>` → Task 1 `ddl`/`query_sql`, Task 2 `embed_*`. ✓
- docker-compose.yml → Task 2 Step 2. ✓
- `vector` optional extra, lazy imports → Task 2 Steps 1, 3 (lazy `_embedder`/`connect`/`is_available`). ✓
- serve.py pgvector-with-TF-IDF fallback → Task 3 Steps 3-4. ✓
- Makefile rag targets → Task 2 Step 4. ✓
- Idempotent upsert on note_id → Task 1 `note_id`/`upsert_sql`, Task 2 `load_notes`, integration test. ✓
- Hermetic unit tests (CI) + Docker-gated integration (skips in CI) → Tasks 1, 3 (unit) + Task 2 (integration). ✓
- ADR 0006 + README + serve docstring → Task 3 Steps 4, 6, 7. ✓
- Clone-and-run / hermetic CI unaffected → fallback test (Task 3 Step 1) + integration test skip guard. ✓

**Placeholder scan:** none — all code/SQL/commands are concrete.

**Type consistency:** `is_available()->bool`, `rag_demo(notes, queries)->dict`, `load_notes(df)->int`, `query(text,k)->list[dict]`, `note_id`/`ddl`/`upsert_sql`/`query_sql`/`shape_matches` are defined in Tasks 1-2 and consumed consistently in Task 3 and the tests. `TABLE`/`MODEL`/`DIM` constants referenced consistently.
