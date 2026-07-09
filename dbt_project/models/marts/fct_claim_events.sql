{{
    config(
        materialized='incremental',
        unique_key='claim_event_key',
    )
}}

-- Grain: one row per claim status transition (insert-only event stream).
-- Late-arriving events carry a later loaded_at, so the watermark catches them.
with claim_events as (

    select *
    from {{ ref('int_claim_events_deduplicated') }}
    {% if is_incremental() %}
    where loaded_at > (
        select coalesce(max(loaded_at), cast('1900-01-01' as date)) - interval 14 day
        from {{ this }}
    )
    {% endif %}

),

customer_versions as (select * from {{ ref('dim_customer') }})

select
    {{ dbt_utils.generate_surrogate_key(['evt.claim_number', 'evt.event_sequence']) }} as claim_event_key,
    {{ to_date_key('evt.event_date') }} as date_key,
    coalesce(cust_pit.customer_key, cust_first.customer_key) as customer_key,
    pol.policy_key,
    prod.product_key,
    br.branch_key,
    evt.claim_number,
    evt.event_sequence,
    evt.event_status,
    evt.event_date,
    evt.incident_date,
    evt.claimed_amount,
    evt.assessed_amount,
    evt.paid_amount,
    evt.loaded_at
from claim_events as evt
left join {{ ref('dim_policy') }} as pol
    on evt.policy_number = pol.policy_number
left join {{ ref('dim_product') }} as prod
    on pol.product_code = prod.product_code
left join {{ ref('dim_branch') }} as br
    on evt.branch_code = br.branch_code
left join customer_versions as cust_pit
    on evt.customer_number = cust_pit.customer_number
    and evt.event_date >= cust_pit.valid_from_date
    and evt.event_date < coalesce(cust_pit.valid_to_date, cast('9999-12-31' as date))
left join customer_versions as cust_first
    on evt.customer_number = cust_first.customer_number
    and cust_first.is_first_version
