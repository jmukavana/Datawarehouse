-- Type 2 SCD from the customers snapshot: one row per customer *version*.
-- Facts resolve the version valid at their event date (point-in-time join).
select
    {{ dbt_utils.generate_surrogate_key(['customer_number', 'dbt_valid_from']) }} as customer_key,
    customer_number,
    national_id,
    full_name,
    phone_number,
    date_of_birth,
    county,
    segment,
    risk_tier,
    kyc_status,
    created_at,
    cast(dbt_valid_from as date) as valid_from_date,
    cast(dbt_valid_to as date) as valid_to_date,
    dbt_valid_to is null as is_current,
    row_number() over (partition by customer_number order by dbt_valid_from) = 1 as is_first_version
from {{ ref('customers_snapshot') }}
