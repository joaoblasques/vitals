-- Cost analytics mart: per condition, conservative-care spend and imaging rate vs surgery rate.
-- The kind of value-based-care metric Sword reports to payers.
with per_patient as (
    select patient_key,
           sum(coalesce(paid, 0))                                            as total_paid,
           max(case when procedure_code in ('72148', '73721') then 1 else 0 end) as had_imaging,
           avg(case when denied then 1.0 else 0.0 end)                       as denial_rate
    from {{ ref('fct_claim') }} group by 1
)
select
    d.primary_condition,
    count(*)                                       as n_patients,
    round(avg(d.surgery_90d), 3)                   as surgery_rate,
    round(avg(coalesce(pp.total_paid, 0)), 0)      as avg_conservative_spend,
    round(avg(coalesce(pp.had_imaging, 0)), 3)     as imaging_rate,
    round(avg(coalesce(pp.denial_rate, 0)), 3)     as claim_denial_rate
from {{ ref('dim_patient') }} d
left join per_patient pp on pp.patient_key = d.patient_key
where d.primary_condition is not null
group by 1
order by avg_conservative_spend desc
