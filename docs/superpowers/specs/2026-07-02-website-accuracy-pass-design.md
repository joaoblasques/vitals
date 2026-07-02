# Design — website accuracy freshness pass

_Date: 2026-07-02 · Status: DRAFT — approved design, not yet implemented · Phase: portfolio surface (make the public site reflect the shipped system)_

> **One-liner:** the public showcase site (joaoblasques.com/vitals) **under-claims** the work — the
> roadmap still calls pgvector "the prod target," Kafka "in prod," Feast an "offline Parquet," the stream
> a "file-source demo," and the dev-log stops at Phase 4 (2026-06-23), missing the 9 units shipped since.
> This pass makes the site **honestly reflect reality** (correct the understatement + fill the gap), with
> every claim verifiable against the repo, and `mkdocs build --strict` green.

## Goal

An interviewer cross-checks the site against the repo. Right now that *loses* credit: the site describes
capabilities as "target"/"demo"/"in prod" that are actually built, tested, and (for the medallion + Kafka)
run live. This unit corrects the public narrative to match the shipped system — accurately, without
overclaiming — across the four high-signal pages, keeping the site's structure, concepts, and
auto-generated catalog intact.

## Non-negotiable principles this serves / preserves

- **Accuracy is the deliverable.** Every changed claim must be backed by the repo (ADRs 0001–0010,
  `data/results.json`, the code, `README`). A wrong claim on a public showcase is the failure mode.
- **Correct understatement AND avoid overstatement.** Stop calling real things "targets/demos"; but honor
  the ADRs' honest scoping — pgvector / Feast / MetricFlow / Kafka are **local**; managed cloud is
  noted-not-exercised; the medallion job is a real Databricks **serverless** run (TERMINATED SUCCESS);
  the Kafka source is a **local Docker** broker (parity-proven, not a managed cluster).
- **Build stays green.** `mkdocs build --strict` (the CI in `.github/workflows/docs.yml`) fails on broken
  links/warnings — all internal links + ADR references must resolve.

## Source of truth (what backs each claim)

The repo, in priority order: the **ADRs** (`docs/adr/0001-0010`), **`data/results.json`** (current pipeline
outputs), the **code** (`src/vitals/`, `dbt/`, `databricks.yml`), and `README.md` / `HANDOFF.md`. The plan
pins the exact facts (test counts, ADR dates, results keys) from these before writing any page.

## The 9 units to reflect (with their repo anchors)

| Capability | Was on site as… | Now (repo anchor) |
|---|---|---|
| pgvector RAG | "TF-IDF; pgvector is the prod target" | real pgvector store + fallback (ADR 0006) |
| dbt semantic layer | absent ("metric mart", "8 tests") | MetricFlow semantic_models + metrics (ADR 0007) |
| Full-medallion job | "Airflow DAG mirroring it" / Phase-5 gold-only | `python_wheel_task` medallion job, live TERMINATED SUCCESS (ADR 0005 update) |
| Feast feature store | "feature table (offline Parquet)" | materialized online + point-in-time historical (ADR 0008) |
| GE silver DQ gate | absent | Great Expectations gates silver in CI (ADR 0009) |
| Kafka stream | "file-source demo; Kafka in prod" | real Kafka source, parity identical 15169 (ADR 0010) |
| Hermetic CI gate | absent | `.github/workflows/ci.yml` (ruff + tests + build + DQ) |
| Failure alerts / drift-as-job | partial | bundle `on_failure` + `drift_monitor` task (ADR 0005 update) |

## Components (pages + changes)

### 1. `website/docs/roadmap.md` (biggest fix)
Mark the 9 units done and correct the false wording. Add a **Phase 6 — three-store gold made real +
governed + streamed** (or update the existing phase checklists) covering: pgvector RAG, MetricFlow
semantic layer, the full-medallion `python_wheel_task` job (live), Feast online+historical, the GE silver
DQ gate, the real Kafka source, and the hermetic CI gate. Fix inline: "pgvector is the prod target" →
real; "Kafka in prod" → exercised (local, parity-proven); "Feast … offline Parquet" → materialized
online+PIT; "8 tests" → the current dbt-test count; "TF-IDF" note updated. Keep the phase-based structure.

### 2. `website/docs/dev-log.md`
Prepend dated entries (dates from the ADRs, 2026-06-25 → 2026-07-02) for the units — each a short
what/why + a link to its ADR. Keep the existing Phase 0–4 entries below.

### 3. `website/docs/results.md`
Update to the current `data/results.json` shape: `feature_store` (n_patients + `online_parity` /
`historical_parity` all_match), `vector_index` (pgvector RAG matches), `model` (MLflow metrics),
`data_quality`, and note the GE `ge_validation` gate + the drift report. Numbers taken from an actual
`make run` output (the plan regenerates/reads `results.json`).

### 4. `website/docs/architecture.md`
Reflect the real system: BRONZE→SILVER (PHI boundary, GE-gated)→GOLD **three real stores** (dbt marts +
MetricFlow semantic layer · Feast online+offline · pgvector) → PROVE; the **medallion Asset Bundle job**
(generate→bronze→silver→gold→drift, serverless, no laptop); the **Kafka** stream source. Keep the medallion
framing; correct the store descriptions + add the deploy/streaming reality.

### 5. `website/docs/index.md` + `website/docs/governance.md` (light)
Only fix wrong/understated claims: `governance.md` should name the **GE silver DQ gate** and drift-as-a-job
alongside the PHI grants; `index.md`'s pitch line should match the shipped three-store-real system. No
structural change.

## Verification

- **Build gate:** `cd website && mkdocs build --strict` → clean (no broken links/nav warnings). This is
  the same command CI (`docs.yml`) runs.
- **Accuracy review:** each changed claim is cross-checked against its repo anchor (ADR / results.json /
  code). The review's job is factual correctness (no over/under-claim), not prose polish.
- **No regressions:** concepts/, catalog.md, nav, and `website/site/` are untouched.

## Non-goals (YAGNI)

- No visual/theme redesign, no new pages, no nav restructure.
- No changes to `concepts/` (FHIR/PHI/OMOP — stable) or the auto-generated `catalog.md`.
- No edits to `website/site/` (build output; CI builds + deploys it).
- No new claims beyond what the repo backs; no managed-cloud/production overclaiming.

## Files touched

| File | Change |
|---|---|
| `website/docs/roadmap.md` | mark 9 units done; fix understated wording; Phase 6 |
| `website/docs/dev-log.md` | dated entries for the units (→ ADR links) |
| `website/docs/results.md` | current `results.json` outputs + GE/drift note |
| `website/docs/architecture.md` | three real stores + medallion job + Kafka source |
| `website/docs/index.md` | pitch line matches reality (light) |
| `website/docs/governance.md` | name the GE DQ gate + drift-as-job (light) |
