select
    policy_number,
    cast(installment_no as integer) as installment_no,
    cast(due_date as date) as due_date,
    cast(amount_due as decimal(14, 2)) as amount_due,
    cast(created_at as date) as created_at,
    cast(_load_date as date) as loaded_at
from {{ source('core_insurance', 'premium_schedules') }}
qualify row_number() over (
    partition by policy_number, installment_no
    order by loaded_at
) = 1
