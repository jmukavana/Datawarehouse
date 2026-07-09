with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2025-01-01' as date)",
        end_date="cast('2028-01-01' as date)"
    ) }}

),

calendar as (

    select
        cast(date_day as date) as date_day,
        cast(extract(year from date_day) as integer) as year,
        cast(extract(month from date_day) as integer) as month,
        cast(extract(day from date_day) as integer) as day_of_month,
        cast(extract(isodow from date_day) as integer) as iso_day_of_week
    from spine

)

select
    {{ to_date_key('date_day') }} as date_key,
    date_day,
    year,
    month,
    strftime(date_day, '%B') as month_name,
    day_of_month,
    iso_day_of_week,
    strftime(date_day, '%A') as day_name,
    iso_day_of_week in (6, 7) as is_weekend,
    -- Fixed-date Kenyan public holidays.
    strftime(date_day, '%m-%d') in
        ('01-01', '05-01', '06-01', '10-10', '10-20', '12-12', '12-25', '12-26')
        as is_holiday,
    -- Kenyan fiscal year runs July to June.
    case when month >= 7 then year else year - 1 end as fiscal_year_start,
    'FY' || cast(case when month >= 7 then year else year - 1 end as varchar)
        || '/' || substr(cast(case when month >= 7 then year + 1 else year end as varchar), 3, 2)
        as fiscal_year_label
from calendar
