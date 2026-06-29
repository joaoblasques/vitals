# Design — Real pgvector vector index (retrieval-only RAG)

_Date: 2026-06-29 · Status: DRAFT — approved design, not yet implemented · Phase: gold serving stores (closes the TF-IDF→pgvector gap)_

> **One-liner:** replace the in-memory TF-IDF placeholder in `serve.py` with a real **pgvector**
> vector store — semantic embeddings (fastembed `bge-small-en-v1.5`, 384-d) of clinical notes,
> indexed in Postgres+pgvector (Docker), queried by cosine ANN. Wired into the serving layer with a
> **graceful TF-IDF fallback** so clone-and-run and hermetic CI keep working with no Docker.
> Retrieval-only (no LLM generation) — the RAG demo proves the data is AI-ready, it is not an LLM
> project.

## Goal

Light up one of the three gold serving stores for real. The vector index is today a TF-IDF +
in-memory cosine demo (`serve._rag_demo`) explicitly tagged *"prod target: pgvector + clinical
embeddings"*. This unit makes it a genuine vector database: dense semantic embeddings persisted in
Postgres/pgvector with an HNSW cosine index and ANN retrieval — the local analog of the managed
Postgres that the `vitals_gold.vectors` UC schema documents conceptually.

## Non-negotiable principles this serves / preserves

- **Reproducible from code** — the store stands up from a committed `docker-compose.yml`; load/query
  via `make` targets and a module CLI. No manual GUI.
- **Clone-and-run stays the default** — pgvector is an **optional** local store. With Docker down or
  the `vector` extra uninstalled, `make run` still completes via the TF-IDF fallback.
- **DQ gates / hermetic CI unaffected** — the credential-free, Docker-free CI gate (`make build`,
  which already skips serve) is untouched; pgvector unit tests are hermetic, the integration test is
  Docker-gated and skips in CI.
- **Idempotent pipelines** — re-loading notes upserts on a natural key; row count stable on re-run.
- **Never commit secrets** — only local-dev Postgres creds (`vitals/vitals/vitals` on localhost),
  which are not secrets; overridable by env.

## Scope decisions (locked with the user)

1. **Embeddings:** fastembed `BAAI/bge-small-en-v1.5` (384-d, ONNX/CPU, no torch, no API keys,
   downloads once and caches). Not sentence-transformers (torch weight), not TF-IDF-in-pgvector.
2. **Integration:** real pgvector store wired into `serve.py` with a **TF-IDF fallback** (not a
   standalone side demo, not a hard replacement that would require Docker for `make run`).
3. **Retrieval-only:** embed query → cosine ANN → top-k notes. No generation/LLM step.

## Current state

| Concern | Today |
|---|---|
| Vector index | `serve._rag_demo`: TF-IDF (`sklearn`) + in-memory `cosine_similarity`, top-3 notes per query |
| Source text | `silver.note` (`patient_key`, `text`) read in `serve.run()` |
| Output | `results.json` → `vector_index: {n_notes_indexed, embedding, vocab_size, demo_queries}` |
| Prod analog | `vitals_gold.vectors` UC schema — documented placeholder, no compute |

## Components

### 1. `docker-compose.yml` (new, repo root)

One service `pgvector` on image `pgvector/pgvector:pg16`, `5432:5432`, env
`POSTGRES_USER/PASSWORD/DB = vitals/vitals/vitals` (local-dev defaults), a healthcheck on
`pg_isready`. Ephemeral (no named volume) — reload is idempotent, so persistence isn't required for
the demo.

### 2. `src/vitals/vector_index.py` (new) — the store's whole interface

I/O separated from pure logic (project test discipline). Connection params from env
(`PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE`) with localhost/`vitals` defaults.

Constants: `MODEL = "BAAI/bge-small-en-v1.5"`, `DIM = 384`, `TABLE = "note_embeddings"`, `TOPK = 3`.

- **Pure (hermetic-testable):**
  - `note_id(patient_key: str, text: str) -> str` — `md5(f"{patient_key}\n{text}")`; the upsert key.
  - `ddl() -> list[str]` — the `CREATE EXTENSION vector`, `CREATE TABLE IF NOT EXISTS note_embeddings
    (note_id text PRIMARY KEY, patient_key text, text text, embedding vector(384))`, and
    `CREATE INDEX IF NOT EXISTS ... USING hnsw (embedding vector_cosine_ops)` statements.
  - `upsert_sql() -> str` / `query_sql(k) -> str` — parameterized SQL
    (`INSERT ... ON CONFLICT (note_id) DO UPDATE SET embedding = EXCLUDED.embedding`;
    `SELECT patient_key, text, 1 - (embedding <=> %s) AS score ORDER BY embedding <=> %s LIMIT %s`).
  - `shape_matches(rows) -> list[dict]` — `[{patient_key, score, note}]` result shaping.
- **I/O (lazy imports of psycopg/pgvector/fastembed):**
  - `embed_documents(texts) -> list[np.ndarray]` — `TextEmbedding(MODEL).embed(texts)`.
  - `embed_query(text) -> np.ndarray` — `.query_embed(text)` (BGE query prefix).
  - `connect()` — `psycopg.connect(...)` + `register_vector(conn)`.
  - `is_available() -> bool` — try a short-timeout connect; any failure → `False` (drives fallback).
  - `load_notes(df) -> int` — run `ddl()`, embed, upsert, return distinct row count. Idempotent.
  - `query(text, k=TOPK) -> list[dict]` — embed query, ANN search, `shape_matches`.
  - `rag_demo(notes_df, queries) -> dict` — `load_notes` then `query` each; returns the serving
    shape `{n_notes_indexed, embedding: "BAAI/bge-small-en-v1.5 (pgvector, cosine)", dim: 384,
    demo_queries: [...]}`.
  - `main()` — CLI: `python -m vitals.vector_index load` | `query "<text>"`.

### 3. `serve.py` (edit `_rag_demo`)

```
try pgvector: from vitals import vector_index as vx; if vx.is_available(): return vx.rag_demo(notes, queries)
except Exception: pass
# fall through to the existing TF-IDF path, with embedding field "TF-IDF (fallback; prod: pgvector)"
```

Same `results.json` key (`vector_index`); the `embedding` field reports which path ran. The TF-IDF
branch is unchanged logic, just relabeled and made the fallback.

### 4. Dependencies — new optional extra

`pyproject.toml`: `vector = ["fastembed>=0.3", "psycopg[binary]>=3.1", "pgvector>=0.3"]`. **Out of
core** — the MVP/CI installs stay light; the pgvector path imports these lazily, so an environment
without the extra simply falls back to TF-IDF.

### 5. `Makefile`

- `rag-up` — `docker compose up -d pgvector` (+ wait for healthy)
- `rag-down` — `docker compose down`
- `rag-load` — `python -m vitals.vector_index load`
- `rag-query` — `python -m vitals.vector_index query "$(Q)"`

## Data flow

```
silver.note ──embed(bge-small)──> upsert note_embeddings(note_id PK, patient_key, text, vector(384))
                                          │  HNSW vector_cosine_ops
query text ──query_embed──> ORDER BY embedding <=> q LIMIT k ──> top-k {patient_key, score, note}
```

`serve.run()`'s RAG step calls `vector_index.rag_demo` when pgvector is reachable, else TF-IDF.

## Error handling / gates

- **pgvector unreachable / extra absent:** `is_available()` / a caught import error → TF-IDF fallback;
  `make run` always completes.
- **Idempotency:** `note_id` PK + `ON CONFLICT DO UPDATE` → re-load yields stable row count.
- **Dimension safety:** column is `vector(384)`; the model is pinned to a 384-d model.

## Testing

- **Hermetic unit (`tests/test_vector_index.py`, runs in CI):**
  - `note_id` determinism + uniqueness (different text → different id; same → same).
  - `ddl()` / `upsert_sql()` / `query_sql()` contain the expected pgvector constructs
    (`vector_cosine_ops`, `<=>`, `ON CONFLICT (note_id)`, `vector(384)`).
  - `shape_matches` maps rows → `{patient_key, score, note}`.
  - **Fallback selection:** monkeypatch `vector_index.is_available` → `False`; assert
    `serve._rag_demo` returns the TF-IDF-labelled result over the same notes (and that an import
    failure of the extra also falls back). psycopg/fastembed are **not** imported in these tests.
- **Docker-gated integration (`tests/test_vector_index_integration.py`, skipped in CI):**
  `pytest.importorskip` the extra; `skip` unless `is_available()`. Load a small fixture of notes,
  assert a semantically-relevant note ranks #1 for a query, and that a second `load_notes` keeps the
  row count stable (idempotent).

## Docs

- New ADR `docs/adr/0006-pgvector-local-serving-store.md` — why a local Dockerized pgvector + an
  optional extra + the TF-IDF fallback; `vitals_gold.vectors` as the prod analog; retrieval-only scope.
- Update `serve.py` module docstring (line 4) and `README.md` (vector-store note).

## Non-goals (YAGNI)

- No generation/LLM step (retrieval-only).
- No embeddings or vector store on Databricks (`vitals_gold.vectors` stays the documented prod analog).
- No online/streaming sync, no reranking, no multi-model embedding choice.
- pgvector not added to push/PR CI (Docker-gated integration test only).

## Files touched

| File | Change |
|---|---|
| `docker-compose.yml` | new — pgvector service |
| `src/vitals/vector_index.py` | new — store interface (pure SQL/id logic + I/O) |
| `src/vitals/serve.py` | `_rag_demo` → pgvector with TF-IDF fallback |
| `pyproject.toml` | new `vector` optional extra |
| `Makefile` | `rag-up`/`rag-down`/`rag-load`/`rag-query` |
| `tests/test_vector_index.py` | new — hermetic unit tests |
| `tests/test_vector_index_integration.py` | new — Docker-gated integration test |
| `docs/adr/0006-pgvector-local-serving-store.md` | new ADR |
| `README.md` | vector-store note |
