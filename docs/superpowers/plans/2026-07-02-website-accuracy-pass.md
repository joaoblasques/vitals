# Website Accuracy Freshness Pass — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Update the public MkDocs site so it honestly reflects the 9 units shipped this session — correcting under-claiming ("pgvector is the prod target", "Kafka in prod", Feast "offline Parquet") and filling the dev-log gap — with every claim backed by the repo and `mkdocs build --strict` green.

**Architecture:** A content pass over four high-signal pages (roadmap, dev-log, results, architecture) plus light fixes on index/governance. This is a PROSE task: each step names the specific stale claim + the repo-anchored correct fact; the implementer edits the markdown to match each page's existing voice (verbatim final markdown isn't pre-written — the page is read and edited in place). The build gate is `mkdocs build --strict`.

**Tech Stack:** MkDocs Material (markdown), `mkdocs build --strict`.

## Global Constraints

- **Accuracy over prose.** Every changed/added claim must be backed by a repo anchor: an ADR (`docs/adr/`), `data/results.json`, or the code. No claim the repo doesn't support.
- **Correct understatement AND avoid overstatement (honor the ADRs' scoping):** pgvector / Feast / MetricFlow / Kafka are **local** (Docker/sqlite/file); managed cloud is noted-not-exercised. The medallion job is a real Databricks **serverless** run (verified TERMINATED SUCCESS). Kafka is a **local Docker** broker (parity-proven, 15169 identical) — NOT a managed cluster.
- **Build gate:** `cd website && uv run --with-requirements requirements.txt mkdocs build --strict` must pass (no broken links/nav warnings). Internal links + ADR references must resolve.
- **Untouched:** `website/docs/concepts/`, the auto-generated `website/docs/catalog.md`, `mkdocs.yml` nav, and `website/site/` (build output; CI deploys it).
- **The 9 units + anchors (verbatim facts to use):**
  - pgvector RAG — real store + TF-IDF fallback; fastembed BGE-small 384-d, HNSW cosine; `make rag-up`; **ADR 0006 (2026-06-29)**.
  - dbt semantic layer — MetricFlow `semantic_models` + 7 metrics over a per-patient base, parity vs the marts; `make metrics-query`; **ADR 0007 (2026-06-29)**.
  - Full-medallion job — a `python_wheel_task` (`medallion_ingest`: generate→bronze→silver, PHI+non-empty gates) → `gold_dbt` → `drift_monitor`, one serverless run, **live TERMINATED SUCCESS**; `databricks bundle run`; **ADR 0005 Update (2026-06-30)**.
  - Feast feature store — materialized offline→**online** (sqlite) + **point-in-time historical** retrieval, parity vs the offline parquet; `make feast-demo`; **ADR 0008 (2026-06-30)**.
  - GE silver DQ gate — Great Expectations **gates silver in CI** (`make dq`): coded-vocab value-sets (icd10 ∈ vocab set, glucose mg/dL) + PHI-boundary + ranges + row-count; **ADR 0009 (2026-07-01)**.
  - Kafka stream — real broker (Docker KRaft) + producer + Spark `readStream.format("kafka")`, **parity identical (15169)**; `make stream-parity`; **ADR 0010 (2026-07-02)**.
  - Hermetic CI gate — `.github/workflows/ci.yml`: ruff + pytest + `make build` + `make dq`, no creds.
  - Failure alerts + drift-as-job — bundle `email_notifications.on_failure` + the `drift_monitor` task (**ADR 0005 Update**).
  - **dbt test count:** read the real number from `make build` output (the `PASS=… data tests` line) — the roadmap's "8 tests" is stale; use the actual count.

---

### Task 1: roadmap.md + dev-log.md (status + chronology)

The two most-stale pages: the roadmap under-claims real capabilities; the dev-log stops at Phase 4 (2026-06-23).

**Files:**
- Modify: `website/docs/roadmap.md`
- Modify: `website/docs/dev-log.md`

- [ ] **Step 1: Read both pages in full** (`website/docs/roadmap.md`, `website/docs/dev-log.md`) to learn the existing voice/structure before editing.

- [ ] **Step 2: Fix the roadmap's under-claiming wording (in place)**

In `website/docs/roadmap.md`, correct these specific stale claims to the current reality (keep the phase structure + checkbox style):
- Phase 1: "Vector index + RAG query over clinical notes (TF-IDF; pgvector is the prod target)" → the vector index is now a **real pgvector store** (fastembed BGE-small 384-d, HNSW cosine), with a TF-IDF fallback when the store is down (ADR 0006).
- Phase 1: "Feast feature table (600×8, offline Parquet + Feast repo)" → Feast is now **materialized offline→online (sqlite) with point-in-time historical retrieval**, parity-proven vs the offline parquet (ADR 0008).
- Phase 1: "dbt tests on the silver/gold gate (8 tests passing)" → the **current dbt-test count** (from `make build`) — and the marts now sit behind a **MetricFlow semantic layer** (ADR 0007).
- Phase 3: "Wearable stream via Spark Structured Streaming (file-source demo, checkpointed sink; Kafka in prod)" → a **real Kafka source** (local Docker KRaft broker) reads `format("kafka")`, **parity-proven identical** (15169) to the file path (ADR 0010).

- [ ] **Step 3: Add a Phase 6 to the roadmap covering the remaining new units**

Append a **"## Phase 6 — three-store gold made real, governed & streamed ✅"** section (matching the phase style) with checked items for the capabilities not already covered by Step 2's inline fixes:
- The **full-medallion `python_wheel_task` job** — one serverless run does generate→bronze→silver→gold→drift, no laptop; verified **TERMINATED SUCCESS** ([ADR 0005 Update](https://github.com/joaoblasques/vitals/blob/main/docs/adr/0005-spark-execution-databricks-connect.md)); failure alerts + drift-as-a-job task.
- The **MetricFlow semantic layer** over the marts (composable metrics; `make metrics-query`) ([ADR 0007](https://github.com/joaoblasques/vitals/blob/main/docs/adr/0007-dbt-semantic-layer.md)).
- **Great Expectations** gates the silver DQ contract in CI (coded-vocabulary value-sets + PHI boundary; `make dq`) ([ADR 0009](https://github.com/joaoblasques/vitals/blob/main/docs/adr/0009-great-expectations-silver-dq.md)).
- **Hermetic CI quality gate** (`.github/workflows/ci.yml`) — ruff + tests + local pipeline + the GE gate, on every push.

(Use the same external-ADR-link style the roadmap already uses for ADR 0005 in Phase 5.)

- [ ] **Step 4: Prepend dated dev-log entries for the units**

In `website/docs/dev-log.md`, add new dated entries ABOVE the existing 2026-06-23 entries (newest first), one per unit, each a 1–3 line what/why with a link to its ADR. Use the ADR dates:
- `## 2026-07-02 — Real Kafka stream source ✅` (ADR 0010; parity identical 15169).
- `## 2026-07-01 — Great Expectations silver DQ gate ✅` (ADR 0009; gates silver in CI).
- `## 2026-06-30 — Feast feature store made real ✅` (ADR 0008; online + point-in-time).
- `## 2026-06-30 — Full-medallion job on Databricks ✅` (ADR 0005 Update; python_wheel_task, live SUCCESS).
- `## 2026-06-29 — dbt semantic layer + real pgvector RAG ✅` (ADR 0007 + ADR 0006).
- `## 2026-06-26 — Databricks deploy path + ops hardening ✅` (ADR 0005; failure alerts, drift-as-job, hermetic CI gate).

- [ ] **Step 5: Build-gate + commit**

```bash
cd website && uv run --with-requirements requirements.txt mkdocs build --strict 2>&1 | tail -5; cd ..
```
Expected: build succeeds, no warnings (strict). Then:
```bash
git add website/docs/roadmap.md website/docs/dev-log.md
git commit -m "docs(site): roadmap + dev-log reflect the shipped 9 units (correct under-claiming)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01F1hdqzju3WbgEALYreWUjk"
```

---

### Task 2: architecture.md + results.md (the system + its outputs)

**Files:**
- Modify: `website/docs/architecture.md`
- Modify: `website/docs/results.md`

- [ ] **Step 1: Read both pages + the current outputs**

Read `website/docs/architecture.md` and `website/docs/results.md`. Then read the real outputs:
```bash
test -f data/results.json || (uv sync --extra dev --extra local --extra vector --extra feast >/dev/null 2>&1; make run >/dev/null 2>&1)
python3 -c "import json; print(json.dumps(json.load(open('data/results.json')), indent=2, default=str))" | head -60
cat data/ge_validation.json 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print('GE:', d['n_expectations']-d['n_failed'], 'of', d['n_expectations'], 'passed')" 2>/dev/null || echo "GE: run make dq"
```
Use the ACTUAL numbers from these outputs (roc_auc, n_patients, parity all_match, etc.) — do not invent.

- [ ] **Step 2: Update `architecture.md` to the real system**

Correct/expand the architecture description (keep the medallion framing + any diagram):
- GOLD is **three real stores**: dbt **star + MetricFlow semantic layer** (ADR 0007) · **Feast** offline+**online** with point-in-time (ADR 0008) · **pgvector** vector index (ADR 0006) — not "targets/demos."
- SILVER carries the **PHI boundary** AND is now **gated by Great Expectations** in CI (coded-vocabulary value-sets; ADR 0009).
- Add the **production deploy** reality: the medallion runs as a **scheduled Databricks serverless job** (Asset Bundle `python_wheel_task` → dbt → drift), verified live (ADR 0005 Update) — alongside the local DuckDB clone-and-run default.
- The **wearable stream** reads from a **real Kafka source** (local broker), parity-proven vs the file path (ADR 0010).
- Do NOT overclaim: keep "local DuckDB is the clone-and-run default"; managed cloud stays out of scope.

- [ ] **Step 3: Update `results.md` to the current `results.json`**

Refresh the results page to the actual output shape + numbers:
- `feature_store`: `n_patients`, the feature list, and the Feast demo's `online_parity` / `historical_parity` (`all_match: true`) — the store now serves online + point-in-time, not just an offline parquet.
- `vector_index`: the **pgvector** RAG (`n_notes_indexed`, `embedding`, `demo_queries`) — real store, TF-IDF fallback noted.
- `model`: MLflow metrics (`roc_auc`, `accuracy`, `top_coefficients`) — use the real values from `results.json`.
- `data_quality`: the bronze→silver DQ report, PLUS a note that **Great Expectations gates silver** (`make dq`, N/N expectations) and **PSI drift** is scored (as a job task).
- Keep any existing table/section structure; just make the numbers + capabilities current.

- [ ] **Step 4: Build-gate + commit**

```bash
cd website && uv run --with-requirements requirements.txt mkdocs build --strict 2>&1 | tail -5; cd ..
git add website/docs/architecture.md website/docs/results.md
git commit -m "docs(site): architecture + results reflect three real gold stores, medallion job, DQ gate, Kafka

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01F1hdqzju3WbgEALYreWUjk"
```

---

### Task 3: index.md + governance.md (light) + final strict build

**Files:**
- Modify: `website/docs/index.md`
- Modify: `website/docs/governance.md`

- [ ] **Step 1: Read both pages**

Read `website/docs/index.md` and `website/docs/governance.md`.

- [ ] **Step 2: Fix `index.md`'s pitch to match reality (light)**

Only where a claim is now wrong/understated: the landing pitch should describe the three gold stores as **real** (analytics marts + semantic layer, Feast online feature store, pgvector index), and may note the medallion runs as a scheduled Databricks job + a Kafka stream source. Keep the page short — no structural change; do not turn it into a changelog.

- [ ] **Step 3: Add the GE gate + drift-as-job to `governance.md` (light)**

Where `governance.md` covers DQ/monitoring/PHI, add: **Great Expectations gates the silver coded-vocabulary DQ contract in CI** (`make dq`; ADR 0009), and **PSI drift** runs as a scheduled job task downstream of gold (ADR 0005 Update) — alongside the existing PHI-classification / Unity Catalog grants content. Don't duplicate the whole DQ story; one accurate paragraph + the ADR link.

- [ ] **Step 4: Final full strict build (all pages) + commit**

```bash
cd website && uv run --with-requirements requirements.txt mkdocs build --strict 2>&1 | tail -8; cd ..
```
Expected: `mkdocs build --strict` succeeds across ALL pages (this is the same gate `.github/workflows/docs.yml` runs). If any internal link/ADR reference is broken, fix it before committing.
```bash
git add website/docs/index.md website/docs/governance.md
git commit -m "docs(site): index pitch + governance name the GE DQ gate; final strict build clean

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01F1hdqzju3WbgEALYreWUjk"
```

---

## Self-Review

**Spec coverage:**
- roadmap under-claiming fixes + Phase 6 → Task 1 Steps 2-3. ✓
- dev-log dated entries for the units → Task 1 Step 4. ✓
- results.md → current results.json → Task 2 Step 3. ✓
- architecture.md → three real stores + medallion job + GE gate + Kafka → Task 2 Step 2. ✓
- index.md + governance.md light fixes → Task 3 Steps 2-3. ✓
- Build gate (`mkdocs build --strict`) → every task's final step + Task 3 Step 4 full build. ✓
- Accuracy discipline (repo anchors, no over/under-claim) → Global Constraints + per-step facts. ✓
- Concepts / catalog / nav / site/ untouched → not in any task's file list. ✓

**Placeholder scan:** No vague "update the page" steps — each names the specific claim + the correct repo-anchored fact. The one deferred value (the dbt test count) is explicitly "read from `make build`," and the results numbers are "read from the actual `results.json`" — concrete instructions, not TBDs. (This is a prose task, so steps specify facts + intent rather than verbatim final markdown — the correct content equivalent of "complete code.")

**Type consistency:** N/A (no code); the ADR numbers/dates + `results.json` keys are used identically across tasks (0005–0010, the dates 06-26→07-02, keys `feature_store`/`vector_index`/`model`/`data_quality`). The `mkdocs build --strict` command is identical in every task.
