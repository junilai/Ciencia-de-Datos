-- Falla si el hecho no tiene exactamente una fila por viaje válido de Silver.
select *
from (
    select
        (select count(*) from {{ ref('fct_trips') }})    as fct_rows,
        (select count(*) from {{ ref('silver_trips') }}) as silver_rows
)
where fct_rows <> silver_rows
