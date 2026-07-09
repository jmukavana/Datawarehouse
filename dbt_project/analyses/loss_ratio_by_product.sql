-- Loss ratio = claims paid / premiums collected, by product line.
with premiums as (
    select prod.line_of_business, sum(pay.amount_paid) as premiums_collected
    from {{ ref('fct_premium_payments') }} as pay
    inner join {{ ref('dim_product') }} as prod using (product_key)
    group by 1
),

claims as (
    select prod.line_of_business, sum(evt.paid_amount) as claims_paid
    from {{ ref('fct_claim_events') }} as evt
    inner join {{ ref('dim_product') }} as prod using (product_key)
    where evt.event_status = 'PAID'
    group by 1
)

select
    premiums.line_of_business,
    premiums.premiums_collected,
    coalesce(claims.claims_paid, 0) as claims_paid,
    round(coalesce(claims.claims_paid, 0) / premiums.premiums_collected, 3) as loss_ratio
from premiums
left join claims using (line_of_business)
order by loss_ratio desc
