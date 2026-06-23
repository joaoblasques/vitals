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

## Consequences
- Everything downstream of silver is safe by construction; the boundary is explicit and testable.
- Per-patient date-shifting keeps time-windowed features valid without exposing real dates.
- In production this maps to Unity Catalog column masks + row filters at the same boundary.
