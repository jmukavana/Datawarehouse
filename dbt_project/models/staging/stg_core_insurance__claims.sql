select
    claim_number,
    policy_number,
    customer_number,
    branch_code,
    cast(event_sequence as integer) as event_sequence,
    event_status,
    cast(event_date as date) as event_date,
    cast(incident_date as date) as incident_date,
    cast(claimed_amount as decimal(14, 2)) as claimed_amount,
    cast(assessed_amount as decimal(14, 2)) as assessed_amount,
    cast(paid_amount as decimal(14, 2)) as paid_amount,
    cast(_load_date as date) as loaded_at
from {{ source('core_insurance', 'claims') }}
