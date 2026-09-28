-- Viajes válidos y deduplicados.
select * exclude (rejection_reason, dup_rank)
from {{ ref('silver_trips_evaluated') }}
where rejection_reason is null
  and dup_rank = 1
