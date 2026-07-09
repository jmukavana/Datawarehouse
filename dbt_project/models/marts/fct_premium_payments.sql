{{
    config(
        materialized='incremental',
        unique_key='payment_key',
    )
}}

-- Grain: one row per premium payment received (post-dedup receipt).
with payments as (

    select *
    from {{ ref('int_payments_deduplicated') }}
    {% if is_incremental() %}
    -- 14-day lookback re-scans recent loads so late/duplicate arrivals are
    -- reconciled; unique_key merge keeps the reprocessed rows idempotent.
    where loaded_at > (
        select coalesce(max(loaded_at), cast('1900-01-01' as date)) - interval 14 day
        from {{ this }}
    )
    {% endif %}

),

customer_versions as (select * from {{ ref('dim_customer') }}),
agent_versions as (select * from {{ ref('dim_agent') }})

select
    {{ dbt_utils.generate_surrogate_key(['pay.receipt_number', 'pay.source_system']) }} as payment_key,
    {{ to_date_key('pay.payment_date') }} as date_key,
    {{ to_date_key('sched.due_date') }} as due_date_key,
    -- Point-in-time SCD2 resolution: the customer/agent version valid on the
    -- payment date; falls back to the earliest known version for events that
    -- predate the first snapshot capture.
    coalesce(cust_pit.customer_key, cust_first.customer_key) as customer_key,
    pol.policy_key,
    prod.product_key,
    coalesce(agent_pit.agent_key, agent_first.agent_key) as agent_key,
    br.branch_key,
    chan.payment_channel_key,
    pay.receipt_number,
    sched.amount_due as premium_due_amount,
    pay.amount as amount_paid,
    datediff('day', sched.due_date, pay.payment_date) as collection_lag_days,
    pay.paid_at_utc,
    pay.payment_date,
    pay.loaded_at
from payments as pay
left join {{ ref('stg_core_insurance__premium_schedules') }} as sched
    on pay.policy_number = sched.policy_number
    and pay.installment_no = sched.installment_no
left join {{ ref('dim_policy') }} as pol
    on pay.policy_number = pol.policy_number
left join {{ ref('dim_product') }} as prod
    on pol.product_code = prod.product_code
left join {{ ref('dim_branch') }} as br
    on pol.branch_code = br.branch_code
left join {{ ref('dim_payment_channel') }} as chan
    on pay.channel = chan.channel_code
left join customer_versions as cust_pit
    on pol.customer_number = cust_pit.customer_number
    and pay.payment_date >= cust_pit.valid_from_date
    and pay.payment_date < coalesce(cust_pit.valid_to_date, cast('9999-12-31' as date))
left join customer_versions as cust_first
    on pol.customer_number = cust_first.customer_number
    and cust_first.is_first_version
left join agent_versions as agent_pit
    on pol.agent_code = agent_pit.agent_code
    and pay.payment_date >= agent_pit.valid_from_date
    and pay.payment_date < coalesce(agent_pit.valid_to_date, cast('9999-12-31' as date))
left join agent_versions as agent_first
    on pol.agent_code = agent_first.agent_code
    and agent_first.is_first_version
