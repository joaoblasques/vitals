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
       coalesce(c.denial_rate, 0) as denial_rate,
       current_date::date          as metric_date   -- snapshot date; required by MetricFlow agg_time_dimension
from {{ ref('dim_patient') }} d
left join obs o using (patient_key)
left join clm c using (patient_key)
