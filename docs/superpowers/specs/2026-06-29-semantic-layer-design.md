# Design — dbt Semantic Layer (MetricFlow) over the star

_Date: 2026-06-29 · Status: DRAFT — approved design, not yet implemented · Phase: gold analytics marts (makes the "+ semantic layer" the README advertises real)_

> **One-liner:** replace the hand-rolled aggregations in `mart_condition_outcomes`/`mart_cost_outcomes`
> with a real dbt **Semantic Layer** — declarative `semantic_models` + `metrics` (MetricFlow) over a
> shared patient-grain base table — and prove `mf query` reproduces the existing mart numbers
> (parity). The marts stay (they feed serve/results/website); they become thin `group by`s of the
> same base, so the metric logic lives in one place.

## Goal

The README advertises "analytics marts — **dbt (Kimball star) + semantic layer**", but there is no
semantic layer: `mart_condition_outcomes`/`mart_cost_outcomes` hard-code their aggregations in SQL
(the model comment literally calls itself "the semantic surface"). This unit adds a genuine,
queryable semantic layer (MetricFlow) so metrics are defined once, declaratively, and composable by
any dimension — the senior-DE way to serve consistent metrics.

## Non-negotiable principles this serves / preserves

- **DRY / single source of truth** — the per-patient aggregation logic currently duplicated across
  the two marts is extracted to one base model that the marts and the semantic layer share.
- **Reproducible + verifiable** — metrics validate from code (`mf validate-configs`); a parity test
  proves `mf query` equals the mart rows. `dbt build` natively parses the YAML, so CI gates validity.
- **Clone-and-run / hermetic CI unaffected** — MetricFlow is an **optional extra**; the parity test
  skips when it (or a built warehouse) is absent, exactly like the pgvector integration test.
- **Verify every step against row counts** — the mart refactor must keep mart outputs byte-identical
  (the 26 dbt tests + the gold parity baseline are the safety net).

## Scope decision (locked with the user)

**Keep the marts, add the semantic layer, parity-prove.** Do NOT remove the marts or repoint
downstream consumers (serve.py feature SQL, results.json, website) — that would ripple into the
serving layer and is out of scope. The marts are refactored only to source from the new base model,
preserving their outputs.

## The parity subtlety (why a patient-grain base is needed)

The marts average **per-patient first**: `avg_pain` = the mean over patients of each patient's mean
pain. A naive MetricFlow measure `avg(value_std)` on `fct_observation` grouped by condition would
instead average over all *observations* — a different number. To reproduce the mart numbers, the
semantic measures must aggregate over a **patient-grain** table (one row per patient). Hence the
shared base model below.

## Current state

| Model | Grain | Metrics (hand-rolled) |
|---|---|---|
| `mart_condition_outcomes` | per primary_condition | n_patients, surgery_rate, avg_pain, avg_adherence_pct |
| `mart_cost_outcomes` | per primary_condition | n_patients, surgery_rate, avg_conservative_spend, imaging_rate, claim_denial_rate |

Both inline the same per-patient CTEs (pain/adherence avg per patient; per-patient claim rollup).

## Components

### 1. `dbt/models/fct_patient_metrics.sql` (new) — shared per-patient base

One row per patient, materialized as a table (the per-patient CTEs the marts already compute):

```sql
-- Per-patient analytics base: one row per patient with the measures the marts + semantic layer
-- share. Single source of truth for per-patient aggregation (the marts group by it; the semantic
-- models measure over it).
with obs as (
    select patient_key,
           avg(value_std) filter (where metric = 'pain')      as mean_pain,
           avg(value_std) filter (where metric = 'adherence') as mean_adherence
    from {{ ref('fct_observation') }} group by 1
),
clm as (
    select patient_key,
           sum(coalesce(paid, 0))                                              as total_paid,
           max(case when procedure_code in ('72148','73721') then 1 else 0 end) as had_imaging,
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

Add schema tests in `_schema.yml`: `patient_key` unique+not_null; `surgery_90d` accepted_values [0,1].

### 2. Refactor the two marts to source from `fct_patient_metrics`

`mart_condition_outcomes` and `mart_cost_outcomes` become thin `group by primary_condition` over
`{{ ref('fct_patient_metrics') }}` instead of re-deriving the per-patient CTEs. **Outputs must be
byte-identical** — verified by the 26 dbt tests passing and the gold parity baseline still matching.
(`avg_pain` averages `mean_pain` over patients, etc. — same as today.)

### 3. `dbt/models/_semantic_models.yml` (new) — semantic models + metrics

Classic MetricFlow form (`semantic_models:` + `metrics:`), over `fct_patient_metrics`:

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
  - {name: patient_count,           type: simple, label: "Patients",               type_params: {measure: patients}}
  - {name: surgery_rate,            type: simple, label: "Surgery rate (90d)",      type_params: {measure: surgery_rate_measure}}
  - {name: avg_pain,                type: simple, label: "Avg pain",                type_params: {measure: pain}}
  - {name: avg_adherence,           type: simple, label: "Avg adherence %",        type_params: {measure: adherence}}
  - {name: avg_conservative_spend,  type: simple, label: "Avg conservative spend", type_params: {measure: conservative_spend}}
  - {name: imaging_rate,            type: simple, label: "Imaging rate",           type_params: {measure: imaging}}
  - {name: claim_denial_rate,       type: simple, label: "Claim denial rate",      type_params: {measure: denials}}
```

`surgery_rate` is the mean of the 0/1 `surgery_90d` flag per group (`agg: average`), which equals the
mart's `round(avg(surgery_90d), 3)`. The parity test asserts `surgery_rate` == the mart's
`surgery_rate` (to the mart's rounding); the values are compared with a small float tolerance to
absorb the marts' `round()` vs MetricFlow's unrounded output.

### 4. Optional extra + Makefile

- `pyproject.toml`: `metrics = ["dbt-metricflow[duckdb]"]` (out of core).
- `make` targets: `metrics-validate` (`mf validate-configs`), `metrics-list` (`mf list metrics`),
  `metrics-query` (example: `mf query --metrics surgery_rate,avg_pain --group-by patient__primary_condition`).
  MetricFlow's `mf` runs against the dbt project's configured DuckDB target.

### 5. Parity test — `tests/test_semantic_layer.py`

Hermetic-gated like the pgvector integration test: `pytest.importorskip("metricflow")` (or skip when
the dbt-built DuckDB warehouse is absent). Runs `mf query` for `surgery_rate`, `avg_pain`,
`avg_conservative_spend` grouped by `primary_condition`, and asserts each equals the corresponding
`mart_condition_outcomes`/`mart_cost_outcomes` row (to the marts' rounding). Skips in CI (no `metrics`
extra), runs locally after `make build` + `uv sync --extra metrics`.

## Data flow

```
dim_patient + fct_observation + fct_claim
        └─► fct_patient_metrics (one row/patient)  ──► mart_condition_outcomes  (group by condition)
                                                   ├─► mart_cost_outcomes       (group by condition)
                                                   └─► semantic_models/metrics ──mf query──► same numbers
```

## Error handling / gates

- **YAML/ref validity:** `dbt build` parses `semantic_models`/`metrics` into the manifest → invalid
  config fails the build (so CI's `make build` gates it natively, no metricflow install needed).
- **Output regression:** the mart refactor is guarded by the 26 dbt tests + gold parity baseline.
- **Metric correctness:** the parity test proves `mf query` == mart numbers.

## Testing

- **CI (hermetic, no metricflow):** `dbt build` (in `make build`) parses the semantic YAML + runs the
  26 dbt tests over the refactored marts — both gate on every push.
- **Local (gated, skips in CI):** `tests/test_semantic_layer.py` parity test via `mf query`; plus
  `make metrics-validate` / `metrics-query` for manual inspection.

## Docs

- New ADR `docs/adr/0007-dbt-semantic-layer.md` — declarative metrics over a patient-grain base;
  DRY refactor of the marts; MetricFlow local (DuckDB) with Databricks-compatible-but-not-exercised;
  parity with the marts as the correctness contract.
- README already lists "+ semantic layer"; add a one-line note on `make metrics-query`.

## Non-goals (YAGNI)

- No time-series / cumulative / derived-window metrics (the marts have none).
- No MetricFlow execution against Databricks (local DuckDB SL is the deliverable; the YAML is
  adapter-agnostic and would work there, but exercising it is out of scope).
- No dbt Cloud / hosted Semantic Layer API.
- No removal of the marts or repointing of serve.py/results/website.
- No new dimensions beyond `primary_condition`/`primary_condition_code`.

## Files touched

| File | Change |
|---|---|
| `dbt/models/fct_patient_metrics.sql` | new — per-patient base |
| `dbt/models/mart_condition_outcomes.sql` | refactor to source from the base (output identical) |
| `dbt/models/mart_cost_outcomes.sql` | refactor to source from the base (output identical) |
| `dbt/models/_semantic_models.yml` | new — semantic_models + metrics |
| `dbt/models/_schema.yml` | add tests for `fct_patient_metrics` |
| `pyproject.toml` | new `metrics` optional extra |
| `Makefile` | `metrics-validate` / `metrics-list` / `metrics-query` |
| `tests/test_semantic_layer.py` | new — gated parity test |
| `docs/adr/0007-dbt-semantic-layer.md` | new ADR |
| `README.md` | one-line `make metrics-query` note |
