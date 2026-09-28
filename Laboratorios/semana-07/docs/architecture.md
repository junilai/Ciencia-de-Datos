# Arquitectura

```mermaid
flowchart LR
    TLC[(TLC CloudFront<br/>yellow_tripdata_YYYY-MM.parquet)]
    subgraph Docker["docker compose (nyc-taxi-elt)"]
        KDB[(Postgres<br/>metadata Kestra)]
        K[Kestra v1.3.37<br/>flow usfq.nyc_taxi.nyc_taxi_elt]
        V["/opt/pipeline-venv<br/>load_month.py · dbt"]
        FD[flow-deployer<br/>publica ./flows vía API]
        K --- KDB
        K --> V
        FD -. import .-> K
    end
    subgraph SF["Snowflake · NYC_TAXI (warehouse NYC_WH XSMALL)"]
        ST[[RAW.TLC_STAGE]]
        RAW[(RAW.YELLOW_TRIPDATA)]
        BR[BRONZE<br/>bronze_yellow_tripdata]
        SI[SILVER<br/>silver_trips · silver_trips_rejected]
        GO[GOLD<br/>fct_trips + 6 dimensiones]
    end
    TLC -- "1 · HEAD + download (ForEach 20 meses)" --> V
    V -- "2 · PUT" --> ST
    ST -- "3 · DELETE período + COPY INTO (1 transacción)" --> RAW
    RAW --> BR --> SI --> GO
    V -- "4 · dbt build (modelos + tests)" --> SI
```

| Paso | Componente | Idempotencia |
|---|---|---|
| Ingesta | `ingestion/load_month.py` | DELETE del período + COPY en una transacción; verifica filas parquet = filas cargadas |
| Bronze | view dbt | reflejo 1:1 de RAW |
| Silver / Gold | tablas dbt | reconstrucción completa en cada `dbt build` |
