-- Wearable daily fact: cleaned step/activity/sleep series (outlier steps already nulled at silver).
select
    patient_key,
    day,
    steps,
    active_minutes,
    resting_hr,
    sleep_hours
from {{ source('silver', 'wearable_daily') }}
