-- Patient-reported outcome fact: Oswestry Disability Index over time (valid scores only).
select
    patient_key,
    survey_date,
    instrument,
    score
from {{ source('silver', 'pro') }}
where score is not null
