select
    product_code,
    product_name,
    line_of_business,
    underwriter,
    cast(commission_rate as decimal(5, 4)) as commission_rate,
    cast(_load_date as date) as loaded_at
from {{ source('core_insurance', 'products') }}
qualify row_number() over (partition by product_code order by loaded_at) = 1
