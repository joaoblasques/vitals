-- Conformed patient dimension: one row per de-identified patient, with their
-- primary condition and the ML target. Kimball-style dim for analytics + features.
with cond as (
    select patient_key, icd10_code, display,
           row_number() over (partition by patient_key order by icd10_code) as rn
    from {{ source('silver', 'condition') }}
    where icd10_code is not null
)
select
    p.patient_key,
    p.gender,
    p.age,
    c.icd10_code as primary_condition_code,
    c.display    as primary_condition,
    t.surgery_90d
from {{ source('silver', 'patient') }} p
left join cond c on c.patient_key = p.patient_key and c.rn = 1
left join {{ source('silver', 'target') }} t on t.patient_key = p.patient_key
