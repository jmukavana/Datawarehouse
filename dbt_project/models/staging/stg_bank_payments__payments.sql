select
    bank_reference as receipt_number,
    payer_name,
    split_part(billing_reference, '/', 1) as policy_number,
    cast(split_part(billing_reference, '/', 2) as integer) as installment_no,
    -- Amounts arrive as strings with thousands separators ("12,500.00").
    cast(replace(amount, ',', '') as decimal(14, 2)) as amount,
    -- Value dates arrive in mixed ISO and D/M/Y formats.
    coalesce(
        try_cast(value_date as date),
        cast(try_strptime(value_date, '%d/%m/%Y') as date)
    ) as payment_date,
    cast(coalesce(
        try_cast(value_date as date),
        cast(try_strptime(value_date, '%d/%m/%Y') as date)
    ) as timestamp) as paid_at_utc,
    cast(null as varchar) as paybill_ref,
    channel,
    'bank_payments' as source_system,
    cast(_load_date as date) as loaded_at
from {{ source('bank_payments', 'payments') }}
