with versions as (
    select agent_code, valid_from_date, valid_to_date
    from {{ ref('dim_agent') }}
)

select a.agent_code
from versions as a
inner join versions as b
    on a.agent_code = b.agent_code
    and a.valid_from_date < b.valid_from_date
where a.valid_to_date is null
   or a.valid_to_date > b.valid_from_date
