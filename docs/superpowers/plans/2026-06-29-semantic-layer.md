# dbt Semantic Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a real dbt Semantic Layer (MetricFlow `semantic_models` + `metrics`) over a shared per-patient base model, refactor the two marts to source from that base (outputs identical), and prove `mf query` reproduces the mart numbers.

**Architecture:** A new `fct_patient_metrics` (one row per patient) becomes the single source of per-patient aggregation. `mart_condition_outcomes`/`mart_cost_outcomes` are refactored to `group by` it (byte-identical output, guarded by the dbt tests). `_semantic_models.yml` defines semantic models + 7 metrics over the base; `dbt build` parses them natively (CI gates validity for free); MetricFlow is an optional `metrics` extra used locally for `mf validate-configs`, `mf query`, and a gated parity test.

**Tech Stack:** dbt-duckdb (existing), dbt-metricflow[duckdb] (new optional extra), MetricFlow `mf` CLI, DuckDB, pytest.

## Global Constraints

- Keep the marts and all downstream consumers (serve.py, results.json, website) — refactor marts only to source from `fct_patient_metrics`; their **output must stay byte-identical**.
- MetricFlow is an **optional extra `metrics`** — NOT core. `dbt build` must still pass WITHOUT the extra installed (dbt-core parses semantic models natively); this is what keeps the hermetic CI gate working.
- The parity test skips when the `metrics` extra or the built DuckDB warehouse is absent (like the pgvector integration test) — it does not run in CI.
- 7 metrics: `patient_count`, `surgery_rate`, `avg_pain`, `avg_adherence`, `avg_conservative_spend`, `imaging_rate`, `claim_denial_rate`, defined over `fct_patient_metrics` via measures (count/sum/average).
- `surgery_rate` = `agg: average` of the 0/1 `surgery_90d` flag (== mart's `round(avg(surgery_90d),3)`).
- dbt dev target is DuckDB at `../data/vitals.duckdb`, schema `gold`; `mf` runs from `dbt/` with `DBT_PROFILES_DIR=.`.
- Group-by dimension syntax: `patient__primary_condition` (entity `patient`, dimension `primary_condition`).

---

### Task 1: `fct_patient_metrics` base + mart refactor (output-identical)

Extract the per-patient aggregation the two marts duplicate into one base model, repoint the marts at it, and prove the marts' output is unchanged.

**Files:**
- Create: `dbt/models/fct_patient_metrics.sql`
- Modify: `dbt/models/mart_condition_outcomes.sql` (replace its CTEs + FROM)
- Modify: `dbt/models/mart_cost_outcomes.sql` (replace its CTE + FROM)
- Modify: `dbt/models/_schema.yml` (add `fct_patient_metrics` tests)

**Interfaces:**
- Produces: model `fct_patient_metrics` — one row per patient with columns `patient_key`, `primary_condition`, `primary_condition_code`, `surgery_90d`, `mean_pain`, `mean_adherence`, `total_paid`, `had_imaging`, `denial_rate`.

- [ ] **Step 1: Capture the current mart outputs (the parity baseline for this refactor)**

Run (the marts must be built first; `make build` if `data/vitals.duckdb` is stale):
```bash
make build
uv run python -c "import duckdb; c=duckdb.connect('data/vitals.duckdb'); \
open('/tmp/marts_before.txt','w').write(\
c.sql('select * from gold.mart_condition_outcomes order by primary_condition').df().to_csv(index=False)+\
'===\n'+c.sql('select * from gold.mart_cost_outcomes order by primary_condition').df().to_csv(index=False))"
```
Expected: `/tmp/marts_before.txt` written (no error). This is the byte-for-byte baseline the refactor must reproduce.

- [ ] **Step 2: Create `dbt/models/fct_patient_metrics.sql`**

```sql
-- Per-patient analytics base: one row per de-identified patient with the measures the marts and the
-- semantic layer share. Single source of truth for per-patient aggregation — the marts group by it,
-- the semantic models measure over it (ADR 0007). Extracted from the CTEs the two marts inlined.
with obs as (
    select patient_key,
           avg(value_std) filter (where metric = 'pain')      as mean_pain,
           avg(value_std) filter (where metric = 'adherence') as mean_adherence
    from {{ ref('fct_observation') }} group by 1
),
clm as (
    select patient_key,
           sum(coalesce(paid, 0))                                              as total_paid,
           max(case when procedure_code in ('72148', '73721') then 1 else 0 end) as had_imaging,
           avg(case when denied then 1.0 else 0.0 end)                         as denial_rate
    from {{ ref('fct_claim') }} group by 1
)
select d.patient_key,
       d.primary_condition,
       d.primary_condition_code,
       d.surgery_90d,
       o.mean_pain,
       o.mean_adherence,
       coalesce(c.total_paid, 0)  as total_paid,
       coalesce(c.had_imaging, 0) as had_imaging,
       coalesce(c.denial_rate, 0) as denial_rate
from {{ ref('dim_patient') }} d
left join obs o using (patient_key)
left join clm c using (patient_key)
```

- [ ] **Step 3: Refactor `dbt/models/mart_condition_outcomes.sql`**

Replace the ENTIRE file with (the per-patient CTEs now come from the base; output is identical):

```sql
-- Analytics mart: per primary condition, cohort size, surgery rate, and mean pain / adherence.
-- Thin group-by over fct_patient_metrics (the per-patient base); the semantic layer (ADR 0007)
-- exposes the same numbers as composable metrics.
select
    primary_condition,
    primary_condition_code,
    count(*)                          as n_patients,
    round(avg(surgery_90d), 3)        as surgery_rate,
    round(avg(mean_pain), 2)          as avg_pain,
    round(avg(mean_adherence), 1)     as avg_adherence_pct
from {{ ref('fct_patient_metrics') }}
where primary_condition is not null
group by 1, 2
order by surgery_rate desc
```

- [ ] **Step 4: Refactor `dbt/models/mart_cost_outcomes.sql`**

Replace the ENTIRE file with:

```sql
-- Cost analytics mart: per condition, conservative-care spend and imaging rate vs surgery rate.
-- Thin group-by over fct_patient_metrics (the per-patient base) — value-based-care metrics.
select
    primary_condition,
    count(*)                           as n_patients,
    round(avg(surgery_90d), 3)         as surgery_rate,
    round(avg(total_paid), 0)          as avg_conservative_spend,
    round(avg(had_imaging), 3)         as imaging_rate,
    round(avg(denial_rate), 3)         as claim_denial_rate
from {{ ref('fct_patient_metrics') }}
where primary_condition is not null
group by 1
order by avg_conservative_spend desc
```

- [ ] **Step 5: Add `fct_patient_metrics` tests to `dbt/models/_schema.yml`**

Append (after the `mart_condition_outcomes` block, keeping 2-space indent under `models:`):

```yaml
  - name: fct_patient_metrics
    description: Per-patient analytics base (one row per patient) — shared by the marts + semantic layer.
    columns:
      - name: patient_key
        tests: [unique, not_null]
      - name: surgery_90d
        tests:
          - accepted_values: {values: [0, 1]}
```

- [ ] **Step 6: Rebuild and verify the marts are byte-identical + tests pass**

```bash
make build
uv run python -c "import duckdb; c=duckdb.connect('data/vitals.duckdb'); \
open('/tmp/marts_after.txt','w').write(\
c.sql('select * from gold.mart_condition_outcomes order by primary_condition').df().to_csv(index=False)+\
'===\n'+c.sql('select * from gold.mart_cost_outcomes order by primary_condition').df().to_csv(index=False))"
diff /tmp/marts_before.txt /tmp/marts_after.txt && echo "MARTS IDENTICAL"
```
Expected: `MARTS IDENTICAL` (empty diff). Also confirm the dbt build summary near the end of `make build` shows the data tests passing with no errors (now includes the 3 new `fct_patient_metrics` tests). If the diff is non-empty, the refactor changed a value — STOP and reconcile before committing.

- [ ] **Step 7: Lint guard (dbt models aren't ruff-checked, but confirm nothing else broke)**

Run: `uv run --extra dev pytest tests/ -q`
Expected: same as before this task — all pass / appropriate skips (the marts feed nothing the tests assert on directly; this confirms no collateral breakage).

- [ ] **Step 8: Commit**

```bash
git add dbt/models/fct_patient_metrics.sql dbt/models/mart_condition_outcomes.sql dbt/models/mart_cost_outcomes.sql dbt/models/_schema.yml
git commit -m "refactor(dbt): extract fct_patient_metrics base; marts group by it (output identical)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01F1hdqzju3WbgEALYreWUjk"
```

---

### Task 2: Semantic models + metrics + `metrics` extra + make targets

Define the semantic layer over the base, confirm `dbt build` parses it WITHOUT MetricFlow installed (the CI-safety invariant), then add the extra + `mf validate-configs` + make targets.

**Files:**
- Create: `dbt/models/_semantic_models.yml`
- Modify: `pyproject.toml` (add `metrics` extra after the `vector` extra, line 27)
- Modify: `Makefile` (`.PHONY` + `metrics-validate`/`metrics-list`/`metrics-query`)

**Interfaces:**
- Consumes: model `fct_patient_metrics` (Task 1).
- Produces: semantic model `patient_metrics`; metrics `patient_count`, `surgery_rate`, `avg_pain`, `avg_adherence`, `avg_conservative_spend`, `imaging_rate`, `claim_denial_rate`.

- [ ] **Step 1: Create `dbt/models/_semantic_models.yml`**

```yaml
semantic_models:
  - name: patient_metrics
    description: Per-patient analytics base — the grain is one row per de-identified patient.
    model: ref('fct_patient_metrics')
    entities:
      - name: patient
        type: primary
        expr: patient_key
    dimensions:
      - name: primary_condition
        type: categorical
      - name: primary_condition_code
        type: categorical
    measures:
      - {name: patients, agg: count, expr: patient_key}
      - {name: surgery_rate_measure, agg: average, expr: surgery_90d}
      - {name: pain, agg: average, expr: mean_pain}
      - {name: adherence, agg: average, expr: mean_adherence}
      - {name: conservative_spend, agg: average, expr: total_paid}
      - {name: imaging, agg: average, expr: had_imaging}
      - {name: denials, agg: average, expr: denial_rate}

metrics:
  - {name: patient_count,          type: simple, label: "Patients",               type_params: {measure: patients}}
  - {name: surgery_rate,           type: simple, label: "Surgery rate (90d)",      type_params: {measure: surgery_rate_measure}}
  - {name: avg_pain,               type: simple, label: "Avg pain",                type_params: {measure: pain}}
  - {name: avg_adherence,          type: simple, label: "Avg adherence %",         type_params: {measure: adherence}}
  - {name: avg_conservative_spend, type: simple, label: "Avg conservative spend",  type_params: {measure: conservative_spend}}
  - {name: imaging_rate,           type: simple, label: "Imaging rate",            type_params: {measure: imaging}}
  - {name: claim_denial_rate,      type: simple, label: "Claim denial rate",       type_params: {measure: denials}}
```

- [ ] **Step 2: Verify `dbt build` parses the semantic YAML WITHOUT the metrics extra (the CI-safety check)**

The current `.venv` does NOT have `dbt-metricflow`. Run:
```bash
make build
```
Expected: build SUCCEEDS (dbt-core parses `semantic_models`/`metrics` natively; a "MetricFlow is not installed" *warning* is fine, an ERROR is not). This proves the hermetic CI gate (`make build`, no metrics extra) still passes with the semantic layer present. If `make build` ERRORS on the semantic YAML without metricflow, STOP and report BLOCKED — the design's "CI gates for free" assumption needs revisiting.

- [ ] **Step 3: Add the `metrics` optional extra**

In `pyproject.toml`, after the `vector = [...]` line (line 27), add:

```toml
# Semantic layer — MetricFlow over the marts' star (ADR 0007); optional, not needed for the MVP.
metrics = ["dbt-metricflow[duckdb]"]
```

- [ ] **Step 4: Install the extra and validate the semantic graph**

```bash
uv sync --extra dev --extra metrics
cd dbt && DBT_PROFILES_DIR=. ../.venv/bin/mf validate-configs; cd ..
```
Expected: `mf validate-configs` reports the configs valid (no validation errors). If `mf` needs a fresh semantic manifest, run `cd dbt && DBT_PROFILES_DIR=. ../.venv/bin/dbt parse` first, then re-run validate.

- [ ] **Step 5: Add the make targets**

In `Makefile`, add to `.PHONY` (after `rag-query`): `metrics-validate metrics-list metrics-query`, and append the targets after the `rag-query` block:

```make
metrics-validate:  ## validate the dbt Semantic Layer configs (needs `uv sync --extra metrics`)
	cd dbt && DBT_PROFILES_DIR=. ../.venv/bin/mf validate-configs

metrics-list:      ## list the defined metrics
	cd dbt && DBT_PROFILES_DIR=. ../.venv/bin/mf list metrics

metrics-query:     ## example: surgery rate + pain by condition (needs `make build` first)
	cd dbt && DBT_PROFILES_DIR=. ../.venv/bin/mf query --metrics surgery_rate,avg_pain --group-by patient__primary_condition
```

- [ ] **Step 6: Smoke-test a query**

```bash
make metrics-query
```
Expected: a table with one row per `primary_condition` and `surgery_rate` + `avg_pain` columns (values in the same ballpark as `mart_condition_outcomes`). This confirms the semantic layer executes end to end against the DuckDB warehouse.

- [ ] **Step 7: Commit**

```bash
git add dbt/models/_semantic_models.yml pyproject.toml Makefile uv.lock
git commit -m "feat(dbt): semantic_models + metrics (MetricFlow) + metrics extra/targets

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01F1hdqzju3WbgEALYreWUjk"
```

---

### Task 3: Gated parity test + ADR 0007 + README

Prove `mf query` equals the mart numbers, and document the decision.

**Files:**
- Create: `tests/test_semantic_layer.py`
- Create: `docs/adr/0007-dbt-semantic-layer.md`
- Modify: `README.md` (one-line note near the `make` commands / vector note)

**Interfaces:**
- Consumes: the `metrics` extra + a built `data/vitals.duckdb` (skips otherwise); metrics `surgery_rate`, `avg_pain`, `avg_conservative_spend`; marts `mart_condition_outcomes`, `mart_cost_outcomes`.

- [ ] **Step 1: Write the gated parity test**

Create `tests/test_semantic_layer.py`:

```python
"""Parity test — the dbt Semantic Layer (MetricFlow) must reproduce the mart numbers.

Gated like the pgvector integration test: needs the `metrics` extra AND the dbt-built DuckDB
warehouse, so it SKIPS in CI (which installs only --extra dev and never builds the warehouse before
pytest). Run locally: `uv sync --extra dev --extra metrics && make build && \
uv run --extra dev --extra metrics pytest tests/test_semantic_layer.py -q`.
"""
import csv
import subprocess
from pathlib import Path

import pytest

pytest.importorskip("metricflow")
import duckdb  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "vitals.duckdb"
DBT = ROOT / "dbt"
MF = ROOT / ".venv" / "bin" / "mf"

pytestmark = pytest.mark.skipif(
    not DB.exists() or not MF.exists(),
    reason="needs `make build` + `uv sync --extra metrics`",
)

# metric -> (mart table, mart column, decimals the mart rounds to)
CASES = {
    "surgery_rate": ("mart_condition_outcomes", "surgery_rate", 3),
    "avg_pain": ("mart_condition_outcomes", "avg_pain", 2),
    "avg_conservative_spend": ("mart_cost_outcomes", "avg_conservative_spend", 0),
}


def _mf_query(metric: str, out: Path) -> dict[str, float]:
    subprocess.run(
        [str(MF), "query", "--metrics", metric,
         "--group-by", "patient__primary_condition", "--csv", str(out)],
        cwd=DBT, env={"DBT_PROFILES_DIR": "."}, check=True,
    )
    rows = {}
    with out.open() as fh:
        for r in csv.DictReader(fh):
            cond = r.get("patient__primary_condition") or r.get("primary_condition")
            val = r.get(metric)
            if cond and val not in (None, "", "None"):
                rows[cond] = float(val)
    return rows


def _mart_values(table: str, col: str) -> dict[str, float]:
    con = duckdb.connect(str(DB))
    df = con.execute(
        f"select primary_condition, {col} from gold.{table} where primary_condition is not null"
    ).df()
    con.close()
    return {row.primary_condition: float(getattr(row, col)) for row in df.itertuples()}


@pytest.mark.parametrize("metric", list(CASES))
def test_metric_matches_mart(metric, tmp_path):
    table, col, decimals = CASES[metric]
    sl = _mf_query(metric, tmp_path / f"{metric}.csv")
    mart = _mart_values(table, col)
    assert set(sl) == set(mart), f"{metric}: condition groups differ"
    for cond, mart_val in mart.items():
        assert round(sl[cond], decimals) == mart_val, (
            f"{metric}[{cond}]: SL {sl[cond]} (round {decimals}) != mart {mart_val}"
        )
```

- [ ] **Step 2: Run the parity test (extra installed + warehouse built)**

```bash
make build   # ensure the marts + base are current in data/vitals.duckdb
uv run --extra dev --extra metrics pytest tests/test_semantic_layer.py -q
```
Expected: PASS (3 parametrized cases — surgery_rate, avg_pain, avg_conservative_spend — each matching the mart to its rounding). If a case mismatches, the assertion prints the condition + both values; reconcile the measure/metric definition (Task 2) before proceeding.

- [ ] **Step 3: Confirm it SKIPS cleanly without the extra (CI behavior)**

```bash
uv run --extra dev pytest tests/test_semantic_layer.py -q
```
Expected: `1 skipped` (or the file's cases skipped) via `importorskip("metricflow")` — proving CI (which lacks the `metrics` extra) won't run or fail on it.

- [ ] **Step 4: Write ADR 0007**

Create `docs/adr/0007-dbt-semantic-layer.md`:

```markdown
# ADR 0007 — dbt Semantic Layer (MetricFlow) over a per-patient base

**Status:** accepted · 2026-06-29

## Context
The README promises "analytics marts — dbt (Kimball star) + semantic layer", but the marts
(`mart_condition_outcomes`/`mart_cost_outcomes`) hard-coded their aggregations in SQL and duplicated
the same per-patient rollups. There was no declarative, composable metric layer — the thing that lets
analytics, BI, and ad-hoc queries share one definition of "surgery rate" or "avg conservative spend".

## Decision
Add a real dbt **Semantic Layer** (MetricFlow): `semantic_models` + `metrics` defined over a new
per-patient base model, `fct_patient_metrics`. The two marts are refactored to `group by` that same
base (outputs unchanged), so the per-patient aggregation lives in exactly one place and the marts and
the semantic layer can't disagree.

Key choices:
- **Per-patient base, not observation-grain.** The marts average per-patient first (mean of patient
  means); semantic measures must aggregate over a one-row-per-patient table to reproduce that. Hence
  `fct_patient_metrics`.
- **MetricFlow is an optional `metrics` extra.** `dbt build` parses the semantic YAML natively, so the
  hermetic, no-extra CI gate validates the config for free. `mf` (validate/query) and the parity test
  are local-only — clone-and-run and CI never depend on MetricFlow.
- **Parity is the correctness contract.** A gated test proves `mf query` equals the mart numbers per
  condition; the marts are the trusted reference.

## Consequences
- New `metrics` dependency group (`dbt-metricflow[duckdb]`); not core. `make metrics-validate` /
  `metrics-list` / `metrics-query` drive it.
- The semantic YAML is adapter-agnostic — it would run against the Databricks target too — but
  exercising MetricFlow on Databricks is out of scope (local DuckDB is the deliverable).
- `fct_patient_metrics` is a new gold table; the marts now depend on it (one extra build node).

## Alternatives considered
- **Observation-grain measures (no base table):** simpler YAML, but the metrics would not match the
  marts' patient-first averaging — no parity, two conflicting definitions of the same metric.
- **Keep hand-rolled marts only:** no composability; "semantic layer" stays a label, not a capability.
- **dbt Cloud / hosted Semantic Layer API:** out of scope for a local, reproducible showcase.
```

- [ ] **Step 5: Add the README note**

In `README.md`, find the line added for the vector store note (the table row ending `TF-IDF fallback otherwise)` at line ~35) and update the **Analytics marts** row (line 33) from:

```markdown
| **Analytics marts** | dbt (Kimball star) + semantic layer | BI, cohorts, clinical/commercial reporting |
```

to:

```markdown
| **Analytics marts** | dbt (Kimball star) + MetricFlow semantic layer | BI, cohorts, reporting (`make metrics-query` for composable metrics) |
```

- [ ] **Step 6: Commit**

```bash
git add tests/test_semantic_layer.py docs/adr/0007-dbt-semantic-layer.md README.md
git commit -m "test+docs(dbt): semantic-layer parity test + ADR 0007 + README

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01F1hdqzju3WbgEALYreWUjk"
```

---

## Self-Review

**Spec coverage:**
- `fct_patient_metrics` per-patient base → Task 1 Step 2. ✓
- Mart refactor, output-identical, guarded → Task 1 Steps 3-6 (before/after diff + dbt tests). ✓
- `semantic_models` + 7 metrics over the base → Task 2 Step 1. ✓
- `surgery_rate` = average of surgery_90d → Task 2 Step 1 (`surgery_rate_measure`). ✓
- dbt build parses YAML without metricflow (CI-safe) → Task 2 Step 2 (explicit check). ✓
- `metrics` optional extra + make targets → Task 2 Steps 3,5. ✓
- Gated parity test, skips in CI → Task 3 Steps 1-3. ✓
- ADR 0007 + README → Task 3 Steps 4-5. ✓
- Non-goals respected (no mart removal, no Databricks mf, no new dims) → no task does these. ✓

**Placeholder scan:** none — all SQL/YAML/commands/test code are concrete.

**Type consistency:** `fct_patient_metrics` columns defined in Task 1 are the exact `expr:` sources in Task 2's measures (`surgery_90d`, `mean_pain`, `mean_adherence`, `total_paid`, `had_imaging`, `denial_rate`, `patient_key`); metric names in Task 2 match the parity test's `CASES` keys in Task 3. The marts' rounded columns (`surgery_rate` 3dp, `avg_pain` 2dp, `avg_conservative_spend` 0dp) match the `decimals` in Task 3's `CASES`.
