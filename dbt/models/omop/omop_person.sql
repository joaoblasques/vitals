-- OMOP CDM v5.4 PERSON. Assigns a stable integer person_id and maps gender to the
-- standard OMOP concept ids (8507 male / 8532 female). year_of_birth derived from age.
select
    row_number() over (order by patient_key)                                   as person_id,
    patient_key,
    case gender when 'male' then 8507 when 'female' then 8532 else 0 end        as gender_concept_id,
    case when age is not null then 2026 - age else null end                     as year_of_birth
from {{ ref('dim_patient') }}
