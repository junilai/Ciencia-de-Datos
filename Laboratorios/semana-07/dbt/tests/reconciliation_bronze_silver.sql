-- Falla si Bronze ≠ Silver + rechazados + duplicados eliminados.
with counts as (
    select
        (select count(*) from {{ ref('bronze_yellow_tripdata') }})    as bronze_rows,
        (select count(*) from {{ ref('silver_trips') }})              as silver_rows,
        (select count(*) from {{ ref('silver_trips_rejected') }})     as rejected_rows,
        (select count(*) from {{ ref('silver_trips_evaluated') }}
          where rejection_reason is null and dup_rank > 1)            as duplicate_rows
)
select *
from counts
where bronze_rows <> silver_rows + rejected_rows + duplicate_rows
