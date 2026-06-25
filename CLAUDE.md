# CLAUDE.md — vitals

Project context for any AI coding session. Keep it tight; this is read every session.

## What this is
**Vitals** — a governed **medallion lakehouse** that ingests messy, multi-source **health data**
(FHIR/Synthea · claims · wearable streams · PRO surveys · clinical notes) and serves it three
ways: **analytics marts** (dbt/Kimball), an **ML feature store** (Feast), and a **vector index**
(pgvector) for RAG. Built to demonstrate Senior-DE skill for a Sword Health-style role.
Stakeholder: health-tech analytics + ML. See `README.md`. Site: https://joaoblasques.com/vitals/

## Working model (one session, root here to build)
Vitals is worked as **one session per project, rooted at its center of gravity** — root in THIS
repo (`~/Dev/vitals`) to build; root in the vault (`~/Dev/second-brain/01_Projects/Vitals/`) for
planning/notes. Whichever root you're in, target the OTHER explicitly (absolute paths, `git -C`).
Global discipline: `~/.claude/CLAUDE.md`. Project tracking + decisions live in the vault brain.
- **Single-writer discipline:** only one agent writes this repo at a time.
- Never commit secrets (this repo is public). Keep vault planning notes in the vault.

## Non-negotiable principles
- **PHI boundary at silver.** Bronze may contain PHI (gated); **de-identification (HIPAA Safe
  Harbor + date-shift) happens at silver** — nothing downstream of silver carries identifiers.
  This boundary is the project's signature; never leak PHI past it.
- **Reproducible from code.** Infra, pipelines, jobs deploy from a terminal. Tool versions pinned
  in `mise.toml`; Python deps via `uv`. Manual GUI is the last resort, documented.
- **Never commit secrets.** PUBLIC repo. Secrets in `.env` (gitignored) / Secret Manager / CI
  secrets. Check `.gitignore` before adding files.
- **Coded-vocabulary data quality.** Standardize to ICD-10/SNOMED/LOINC/RxNorm; OMOP CDM at
  silver. Validate vocabularies as DQ contracts (Great Expectations), not vibes.
- **Idempotent pipelines.** Re-runs/backfills must not duplicate. Run-id overwrite or natural-key
  upsert.
- **DQ gates before exposing data** — validate source + consumption layers in CI.

## Architecture / stack
BRONZE (raw, messy: FHIR/claims/wearable/PRO/notes; schema drift, dupes, unit drift, PHI) →
SILVER (de-identified = PHI boundary; FHIR flattened; codes standardized; OMOP CDM; DQ contracts)
→ GOLD three stores: **analytics marts** (dbt star + semantic layer) · **feature store** (Feast
offline+online) · **vector index** (pgvector) → PROVE (MLflow demo model + RAG demo).
Stack: Databricks (Delta + **Unity Catalog**) · Python+SQL · **dbt** · **Airflow** · **PySpark** +
Structured Streaming · **Kafka** · **Feast** · **pgvector** · **MLflow** · **Great Expectations** ·
**Terraform**. mise + uv for toolchain/deps.

## How to work here (lessons baked in)
- **Do NOT one-shot.** Build incrementally; **verify every step against row counts /
  expectations** — accuracy here is a context + verification problem, not a codegen one.
- **Test discipline.** Transforms get unit + integration tests; prefer writing the DQ/transform
  assertion first. **Separate I/O from transformation logic** so transforms are unit-testable.
- **DQ as CI gates**, not vibes — wired into GitHub Actions so checks can't be skipped.
- Keep metadata (table format, cluster keys, schema, vocab maps) in version control.

## Layout
`src/` · `pipelines/` · `dbt/` · `airflow/` DAGs · `ml/` (Feast/MLflow) · `docs/` · `website/`.

## Commands
```bash
mise install && uv sync     # provision toolchain + deps
uv run pytest               # tests
uv run ruff check .         # lint
```

## Out of scope
Anything outside the DE pipeline + the three gold stores. The MLflow model and RAG demo exist to
*prove the data is AI-ready*, not as ML/LLM projects in their own right.
