{% snapshot agents_snapshot %}
{{
    config(
        schema='snapshots',
        unique_key='agent_code',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}
-- Branch transfers become SCD2 versions so production credits the branch
-- the agent belonged to at the time of sale.
select *
from {{ ref('stg_core_insurance__agents') }}
qualify row_number() over (
    partition by agent_code
    order by updated_at desc, loaded_at desc
) = 1

{% endsnapshot %}
