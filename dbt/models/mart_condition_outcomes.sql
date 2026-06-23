-- Analytics mart + metric definitions: per primary condition, the cohort size,
-- surgery rate, and mean pain / adherence. This is the "semantic" surface analysts consume.
with pain as (
    select patient_key, avg(value_std) as mean_pain
    from {{ ref('fct_observation') }} where metric = 'pain' group by 1
),
adherence as (
    select patient_key, avg(value_std) as mean_adherence
    from {{ ref('fct_observation') }} where metric = 'adherence' group by 1
)
select
    d.primary_condition,
    d.primary_condition_code,
    count(*)                              as n_patients,
    round(avg(d.surgery_90d), 3)          as surgery_rate,
    round(avg(p.mean_pain), 2)            as avg_pain,
    round(avg(a.mean_adherence), 1)       as avg_adherence_pct
from {{ ref('dim_patient') }} d
left join pain p on p.patient_key = d.patient_key
left join adherence a on a.patient_key = d.patient_key
where d.primary_condition is not null
group by 1, 2
order by surgery_rate desc
