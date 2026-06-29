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
