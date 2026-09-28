with hours as (
    select row_number() over (order by seq4()) - 1 as hour_key
    from table(generator(rowcount => 24))
)

select
    hour_key,
    lpad(hour_key, 2, '0') || ':00' as hour_label,
    case
        when hour_key between 0 and 5 then 'madrugada'
        when hour_key between 6 and 11 then 'mañana'
        when hour_key between 12 and 17 then 'tarde'
        else 'noche'
    end as day_part,
    hour_key in (7, 8, 9, 16, 17, 18, 19) as is_rush_hour
from hours
