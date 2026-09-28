-- Registros inválidos con su primer motivo de rechazo (auditoría de limpieza).
select * exclude (dup_rank)
from {{ ref('silver_trips_evaluated') }}
where rejection_reason is not null
