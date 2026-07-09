-- A claim incident cannot predate the policy it is lodged against.
select
    evt.claim_event_key,
    evt.incident_date,
    pol.start_date
from {{ ref('fct_claim_events') }} as evt
inner join {{ ref('dim_policy') }} as pol using (policy_key)
where evt.incident_date < pol.start_date
