-- The M-Pesa gateway re-pushes transactions (same receipt, sometimes in the
-- next day's file). Keep the earliest-loaded occurrence per receipt.
select *
from {{ ref('int_payments_unioned') }}
qualify row_number() over (
    partition by source_system, receipt_number
    order by loaded_at, paid_at_utc
) = 1
