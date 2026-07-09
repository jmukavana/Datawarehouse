-- Type 2 SCD from the agents snapshot: branch transfers create new versions,
-- so production credits the branch the agent belonged to at time of sale.
select
    {{ dbt_utils.generate_surrogate_key(['agent_code', 'dbt_valid_from']) }} as agent_key,
    agent_code,
    full_name,
    license_number,
    branch_code,
    hire_date,
    cast(dbt_valid_from as date) as valid_from_date,
    cast(dbt_valid_to as date) as valid_to_date,
    dbt_valid_to is null as is_current,
    row_number() over (partition by agent_code order by dbt_valid_from) = 1 as is_first_version
from {{ ref('agents_snapshot') }}
