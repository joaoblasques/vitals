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
