-- OMOP CDM CONDITION_OCCURRENCE. ICD-10 source value mapped to a standard condition
-- concept via the concept_map seed; start date proxied by the patient's earliest observation.
with start as (
    select patient_key, min(obs_date) as condition_start_date
    from {{ ref('fct_observation') }} group by 1
)
select
    row_number() over (order by c.patient_key)              as condition_occurrence_id,
    p.person_id,
    coalesce(cm.target_concept_id, 0)                       as condition_concept_id,
    c.icd10_code                                            as condition_source_value,
    cm.target_name                                          as condition_concept_name,
    s.condition_start_date
from {{ source('silver', 'condition') }} c
join {{ ref('omop_person') }} p on p.patient_key = c.patient_key
left join {{ ref('concept_map') }} cm
       on cm.source_vocabulary = 'ICD10' and cm.source_code = c.icd10_code
left join start s on s.patient_key = c.patient_key
where c.icd10_code is not null
