{% snapshot customers_snapshot %}
{{
    config(
        schema='snapshots',
        unique_key='customer_number',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}
-- Snapshot the current state per customer; the timestamp strategy turns
-- risk-tier re-ratings into SCD2 validity windows (dbt_valid_from/_to).
select *
from {{ ref('stg_core_insurance__customers') }}
qualify row_number() over (
    partition by customer_number
    order by updated_at desc, loaded_at desc
) = 1

{% endsnapshot %}
