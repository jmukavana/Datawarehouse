select
    {{ dbt_utils.generate_surrogate_key(['branch_code']) }} as branch_key,
    branch_code,
    branch_name,
    county,
    region,
    tier
from {{ ref('stg_core_insurance__branches') }}
