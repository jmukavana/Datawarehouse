-- SCD2 validity windows for a customer must never overlap: for any two
-- versions a (earlier) and b (later), a must close on or before b opens.
with versions as (
    select customer_number, valid_from_date, valid_to_date
    from {{ ref('dim_customer') }}
)

select a.customer_number
from versions as a
inner join versions as b
    on a.customer_number = b.customer_number
    and a.valid_from_date < b.valid_from_date
where a.valid_to_date is null
   or a.valid_to_date > b.valid_from_date
