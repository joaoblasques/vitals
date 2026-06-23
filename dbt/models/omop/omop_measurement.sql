-- OMOP CDM MEASUREMENT. LOINC-coded observations (glucose, heart rate, pain score)
-- mapped to standard measurement concepts via the concept_map seed.
select
    row_number() over (order by f.patient_key, f.obs_date)  as measurement_id,
    p.person_id,
    cm.target_concept_id                                    as measurement_concept_id,
    cm.target_name                                          as measurement_concept_name,
    f.loinc_code                                            as measurement_source_value,
    f.value_std                                             as value_as_number,
    f.unit_std                                              as unit_source_value,
    f.obs_date                                              as measurement_date
from {{ ref('fct_observation') }} f
join {{ ref('omop_person') }} p on p.patient_key = f.patient_key
join {{ ref('concept_map') }} cm
      on cm.source_vocabulary = 'LOINC' and cm.source_code = f.loinc_code
