{#- The gateway feed gained a paybill_ref column mid-history (schema drift).
    Guard the reference so the model works before and after the column exists. -#}
{%- set column_names = adapter.get_columns_in_relation(source('mpesa_gateway', 'transactions'))
        | map(attribute='name') | map('lower') | list -%}

select
    receipt_number,
    msisdn,
    paybill_number,
    split_part(account_reference, '/', 1) as policy_number,
    cast(split_part(account_reference, '/', 2) as integer) as installment_no,
    cast(amount as decimal(14, 2)) as amount,
    -- Source sends naive Africa/Nairobi wall time which dlt ingested as UTC;
    -- shift -3h (Kenya has no DST) to get the true UTC instant.
    (transaction_time at time zone 'UTC') - interval 3 hour as paid_at_utc,
    cast(transaction_time at time zone 'UTC' as date) as payment_date,
    {% if 'paybill_ref' in column_names %}paybill_ref{% else %}cast(null as varchar) as paybill_ref{% endif %},
    'MPESA' as channel,
    'mpesa_gateway' as source_system,
    cast(_load_date as date) as loaded_at
from {{ source('mpesa_gateway', 'transactions') }}
