with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2025-01-01' as date)",
        end_date="cast('2027-01-01' as date)"
    ) }}
)

select
    to_number(to_char(date_day, 'YYYYMMDD')) as date_key,
    date_day::date                            as full_date,
    year(date_day)                            as year,
    quarter(date_day)                         as quarter,
    month(date_day)                           as month,
    to_char(date_day, 'MMMM')                 as month_name,
    day(date_day)                             as day_of_month,
    dayofweekiso(date_day)                    as day_of_week,
    dayname(date_day)                         as day_name,
    dayofweekiso(date_day) in (6, 7)          as is_weekend
from spine
