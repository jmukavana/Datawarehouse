-- One shape for premium payments regardless of channel/source system.
select
    receipt_number,
    policy_number,
    installment_no,
    amount,
    paid_at_utc,
    payment_date,
    channel,
    source_system,
    loaded_at
from {{ ref('stg_mpesa_gateway__payments') }}

union all

select
    receipt_number,
    policy_number,
    installment_no,
    amount,
    paid_at_utc,
    payment_date,
    channel,
    source_system,
    loaded_at
from {{ ref('stg_bank_payments__payments') }}
