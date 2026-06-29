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
