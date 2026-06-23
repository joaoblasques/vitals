-- Observation fact: one row per measurement, standardized units, typed.
select
    patient_key,
    obs_date,
    metric,
    value_std,
    unit_std
from {{ source('silver', 'observation') }}
