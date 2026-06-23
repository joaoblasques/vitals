-- Claim fact: one row per conservative-care claim line (no surgery codes — leakage-free).
select
    patient_key,
    claim_date,
    procedure_code,
    procedure_display,
    dx_code,
    billed,
    paid,
    denied
from {{ source('silver', 'claim') }}
