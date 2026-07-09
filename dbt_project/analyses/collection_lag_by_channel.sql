-- How long after the due date does money actually arrive, per channel?
select
    chan.channel_name,
    count(*) as payments,
    round(avg(pay.collection_lag_days), 1) as avg_lag_days,
    cast(median(pay.collection_lag_days) as integer) as median_lag_days,
    round(100.0 * count(*) filter (where pay.collection_lag_days <= 3) / count(*), 1)
        as pct_within_3_days
from {{ ref('fct_premium_payments') }} as pay
inner join {{ ref('dim_payment_channel') }} as chan using (payment_channel_key)
group by 1
order by avg_lag_days
