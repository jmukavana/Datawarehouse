-- Premium production credited to the branch the agent belonged to AT THE TIME
-- of each payment (point-in-time SCD2 join) vs their current branch — the two
-- differ for transferred agents, which is the whole point of the Type 2 dim.
select
    agent_pit.agent_code,
    agent_pit.branch_code as branch_at_payment_time,
    agent_now.branch_code as branch_today,
    count(*) as payments,
    sum(pay.amount_paid) as premium_collected
from {{ ref('fct_premium_payments') }} as pay
inner join {{ ref('dim_agent') }} as agent_pit
    on pay.agent_key = agent_pit.agent_key
inner join {{ ref('dim_agent_current') }} as agent_now
    on agent_pit.agent_code = agent_now.agent_code
where agent_pit.branch_code != agent_now.branch_code
group by 1, 2, 3
order by premium_collected desc
