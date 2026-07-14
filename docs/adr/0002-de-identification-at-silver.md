# ADR 0002 — De-identification at the silver boundary

**Status:** accepted · 2026-06-23

## Context
PHI (names, SSNs, addresses, full DOBs) arrives in raw clinical data. Analytics, ML, and RAG must
never touch it, but the raw data must still be landed for fidelity and lineage.

## Decision
Land PHI in **bronze** (access-gated), and make **silver the de-identified boundary**: drop the 18
HIPAA Safe Harbor identifiers, replace the source id with a salted-hash `patient_key`, shift dates
per-patient (preserving intervals), and cap age at 90. A build-time assertion fails the pipeline if
any PHI column survives into silver.

### Salting the key (why a bare hash is not enough)
`patient_key = md5(id || SALT)`. The source ids are a small, guessable/sequential space (Synthea
ids), so an **un-salted** `md5(id)` is trivially reversible by a dictionary/rainbow attack — anyone
could rebuild the id→key map and re-identify the "de-identified" data. The salt is a **single shared
secret**: the *same* value for every patient, row, and table, so the key stays deterministic (same
patient → same `patient_key` everywhere) and cross-table joins on `patient_key` still resolve. A
per-row random salt would defeat that and is explicitly wrong here. The same salt also seeds the
per-patient date shift (`md5(id || SALT)`), so the shift can't be reverse-engineered from the id
either.

**Where the salt lives:** it is read from the `VITALS_SALT` environment variable at runtime (with a
gitignored repo-root `.env` fallback) and is **never committed** to this public repo. `make setup`
generates a random one into `.env`; `.env.example` documents the variable. Every engine reads the
*same* salt via `vitals.salt`, so `patient_key` is identical across the local DuckDB silver, the
Databricks-Delta silver, and the PySpark streaming/scale paths. On Databricks the salt is supplied as
a **job secret** (`{{secrets/...}}` → `VITALS_SALT`), not from the file. CI runs with a throwaway
non-secret salt — it exercises the de-id *logic*, not the secrecy of the value.

## Consequences
- Everything downstream of silver is safe by construction; the boundary is explicit and testable.
- Per-patient date-shifting keeps time-windowed features valid without exposing real dates.
- In production this maps to Unity Catalog column masks + row filters at the same boundary.
- The pipeline now **refuses to run without a salt** (raises rather than silently falling back to an
  un-salted hash) — de-identification can never quietly degrade to the reversible form.

## History
- 2026-06-23 — accepted (salted-hash design).
- 2026-07-14 — **gap closed.** The implementation had been shipping a bare `md5(id)` (no salt) across
  all engines despite this ADR; added `vitals.salt` (env/`.env` → shared secret), salted every
  `patient_key` and the date shift on all four engine paths, and wired `make setup` / `.env.example` /
  CI. Verified: keys changed from the old un-salted output and every cross-table join still resolves.
