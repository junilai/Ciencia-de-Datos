-- Estandariza nombres/tipos, trata nulos, genera trip_id, evalúa reglas de validez y marca duplicados.
-- Es la base común de silver_trips (válidos) y silver_trips_rejected (inválidos).
with bronze as (
    select * from {{ ref('bronze_yellow_tripdata') }}
),

standardized as (
    select
        vendorid::int                                         as vendor_id,
        tpep_pickup_datetime::timestamp_ntz                   as pickup_at,
        tpep_dropoff_datetime::timestamp_ntz                  as dropoff_at,
        passenger_count::int                                  as passenger_count,      -- nulo se mantiene
        trip_distance::number(12, 2)                          as trip_distance,
        coalesce(ratecodeid::int, 99)                         as rate_code_id,         -- 99 = Unknown (TLC)
        case upper(trim(store_and_fwd_flag))
            when 'Y' then true
            when 'N' then false
        end                                                   as store_and_fwd_flag,
        pulocationid::int                                     as pickup_location_id,
        dolocationid::int                                     as dropoff_location_id,
        payment_type::int                                     as payment_type_id,
        fare_amount::number(12, 2)                            as fare_amount,
        extra::number(12, 2)                                  as extra,
        mta_tax::number(12, 2)                                as mta_tax,
        tip_amount::number(12, 2)                             as tip_amount,
        tolls_amount::number(12, 2)                           as tolls_amount,
        improvement_surcharge::number(12, 2)                  as improvement_surcharge,
        total_amount::number(12, 2)                           as total_amount,
        coalesce(congestion_surcharge, 0)::number(12, 2)      as congestion_surcharge, -- nulo = no aplicó
        coalesce(airport_fee, 0)::number(12, 2)               as airport_fee,
        coalesce(cbd_congestion_fee, 0)::number(12, 2)        as cbd_congestion_fee,
        nullif(trim(request_source), '')                      as request_source,
        _source_file,
        _source_period,
        _loaded_at
    from bronze
),

evaluated as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'vendor_id', 'pickup_at', 'dropoff_at', 'pickup_location_id',
            'dropoff_location_id', 'fare_amount', 'total_amount', '_source_period'
        ]) }} as trip_id,
        *,
        case
            when pickup_at is null or dropoff_at is null
                then 'missing_timestamp'
            when pickup_location_id is null or dropoff_location_id is null
                or pickup_location_id not between 1 and 265
                or dropoff_location_id not between 1 and 265
                then 'invalid_location'
            when date_trunc('month', pickup_at)::date <> to_date(_source_period || '-01')
                then 'pickup_out_of_period'
            when dropoff_at <= pickup_at
                then 'non_positive_duration'
            when datediff('minute', pickup_at, dropoff_at) > 1440
                then 'duration_over_24h'
            when trip_distance is null or trip_distance < 0 or trip_distance > 500
                then 'invalid_distance'
            when total_amount is null or total_amount < 0
                then 'negative_or_missing_amount'
        end as rejection_reason
    from standardized
)

select
    *,
    row_number() over (
        partition by trip_id, (rejection_reason is null)
        order by _loaded_at desc, _source_file
    ) as dup_rank
from evaluated
