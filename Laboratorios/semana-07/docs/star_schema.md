# Esquema estrella (GOLD)

**Grano de `fct_trips`:** un viaje válido (una fila de `silver_trips`).

```mermaid
erDiagram
    fct_trips {
        varchar trip_id PK
        int pickup_date_key FK
        int dropoff_date_key FK
        int pickup_hour_key FK
        int pickup_location_id FK
        int dropoff_location_id FK
        int vendor_id FK
        int payment_type_id FK
        int rate_code_id FK
        boolean store_and_fwd_flag
        varchar _source_period
        int passenger_count
        number trip_distance
        number trip_duration_minutes
        number fare_amount
        number extra
        number mta_tax
        number tip_amount
        number tolls_amount
        number improvement_surcharge
        number congestion_surcharge
        number airport_fee
        number cbd_congestion_fee
        number total_amount
    }
    dim_date {
        int date_key PK
        date full_date
        int year
        int quarter
        int month
        varchar month_name
        int day_of_month
        int day_of_week
        varchar day_name
        boolean is_weekend
    }
    dim_time {
        int hour_key PK
        varchar hour_label
        varchar day_part
        boolean is_rush_hour
    }
    dim_location {
        int location_id PK
        varchar borough
        varchar zone
        varchar service_zone
    }
    dim_vendor {
        int vendor_id PK
        varchar vendor_name
    }
    dim_payment_type {
        int payment_type_id PK
        varchar payment_type_name
    }
    dim_rate_code {
        int rate_code_id PK
        varchar rate_code_name
    }
    fct_trips }o--|| dim_date : "pickup_date_key / dropoff_date_key"
    fct_trips }o--|| dim_time : pickup_hour_key
    fct_trips }o--|| dim_location : "pickup_location_id / dropoff_location_id"
    fct_trips }o--|| dim_vendor : vendor_id
    fct_trips }o--|| dim_payment_type : payment_type_id
    fct_trips }o--|| dim_rate_code : rate_code_id
```

- **Métricas:** passenger_count, trip_distance, trip_duration_minutes, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, congestion_surcharge, airport_fee, cbd_congestion_fee, total_amount.
- **Atributos degenerados:** store_and_fwd_flag, _source_period.
- **Llaves:** naturales en las dimensiones (catálogos TLC pequeños y estables); miembros Unknown (-1, 5, 99) evitan FKs huérfanas.
