select
    agent_code,
    full_name,
    license_number,
    branch_code,
    cast(hire_date as date) as hire_date,
    cast(updated_at as timestamp) as updated_at,
    cast(_load_date as date) as loaded_at
from {{ source('core_insurance', 'agents') }}
