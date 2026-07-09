select
    {{ dbt_utils.generate_surrogate_key(['product_code']) }} as product_key,
    product_code,
    product_name,
    line_of_business,
    underwriter,
    commission_rate
from {{ ref('stg_core_insurance__products') }}
