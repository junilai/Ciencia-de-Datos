-- dbt-snowflake crea las columnas del seed sin comillas (LOCATIONID, BOROUGH, ...).
select
    locationid::int as location_id,
    borough,
    zone,
    service_zone
from {{ ref('taxi_zone_lookup') }}
