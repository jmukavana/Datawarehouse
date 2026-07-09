select
    customer_number,
    nullif(national_id, '') as national_id,
    full_name,
    phone_number,
    cast(date_of_birth as date) as date_of_birth,
    county,
    segment,
    risk_tier,
    kyc_status,
    cast(created_at as date) as created_at,
    cast(updated_at as timestamp) as updated_at,
    cast(_load_date as date) as loaded_at
from {{ source('core_insurance', 'customers') }}
