select
    policy_number,
    customer_number,
    product_code,
    agent_code,
    branch_code,
    status,
    cast(start_date as date) as start_date,
    cast(end_date as date) as end_date,
    sum_insured,
    annual_premium,
    payment_frequency,
    cast(created_at as date) as created_at,
    cast(updated_at as timestamp) as updated_at,
    cast(_load_date as date) as loaded_at
from {{ source('core_insurance', 'policies') }}
