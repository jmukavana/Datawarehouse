select
    branch_code,
    branch_name,
    county,
    region,
    cast(tier as integer) as tier,
    cast(_load_date as date) as loaded_at
from {{ source('core_insurance', 'branches') }}
qualify row_number() over (partition by branch_code order by loaded_at) = 1
