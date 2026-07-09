-- Claim events arrive up to 14 days late; keep the first-loaded copy of each
-- (claim, sequence) event in case an extract is ever replayed.
select *
from {{ ref('stg_core_insurance__claims') }}
qualify row_number() over (
    partition by claim_number, event_sequence
    order by loaded_at
) = 1
