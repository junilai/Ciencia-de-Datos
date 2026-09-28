-- Grano: un viaje válido (una fila de silver_trips).
-- Códigos fuera de catálogo caen en el miembro Unknown para no dejar FKs huérfanas.
select
    t.trip_id,
    to_number(to_char(t.pickup_at, 'YYYYMMDD'))  as pickup_date_key,
    to_number(to_char(t.dropoff_at, 'YYYYMMDD')) as dropoff_date_key,
    hour(t.pickup_at)                            as pickup_hour_key,
    t.pickup_location_id,
    t.dropoff_location_id,
    coalesce(v.vendor_id, -1)                    as vendor_id,
    coalesce(p.payment_type_id, 5)               as payment_type_id,
    coalesce(r.rate_code_id, 99)                 as rate_code_id,
    t.store_and_fwd_flag,
    t._source_period,
    t.passenger_count,
    t.trip_distance,
    round(datediff('second', t.pickup_at, t.dropoff_at) / 60, 2) as trip_duration_minutes,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.congestion_surcharge,
    t.airport_fee,
    t.cbd_congestion_fee,
    t.total_amount
from {{ ref('silver_trips') }} t
left join {{ ref('dim_vendor') }} v on t.vendor_id = v.vendor_id
left join {{ ref('dim_payment_type') }} p on t.payment_type_id = p.payment_type_id
left join {{ ref('dim_rate_code') }} r on t.rate_code_id = r.rate_code_id
