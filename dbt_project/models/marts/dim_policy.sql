-- Type 1: latest known state per policy (status transitions overwrite).
select
    {{ dbt_utils.generate_surrogate_key(['policy_number']) }} as policy_key,
    policy_number,
    customer_number,
    product_code,
    agent_code,
    branch_code,
    status,
    start_date,
    end_date,
    sum_insured,
    annual_premium,
    payment_frequency,
    case
        when sum_insured < 500000 then 'UNDER_500K'
        when sum_insured < 2000000 then '500K_TO_2M'
        when sum_insured < 10000000 then '2M_TO_10M'
        else 'OVER_10M'
    end as sum_insured_band,
    created_at
from {{ ref('stg_core_insurance__policies') }}
qualify row_number() over (
    partition by policy_number
    order by updated_at desc, loaded_at desc
) = 1
