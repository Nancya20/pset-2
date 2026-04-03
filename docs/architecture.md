# Arquitectura del Proyecto NY Taxi ELT

## Diagrama de Flujo

```
┌─────────────────────────────────────────────────────────────────────┐
│                        FUENTE EXTERNA                               │
│  TLC CDN: d37ci6vzurychx.cloudfront.net                             │
│  yellow_tripdata_YYYY-MM.parquet  (2015–2025, ~120 archivos)        │
│  taxi+_zone_lookup.csv                                              │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ HTTPS / parquet download
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    MAGE ORCHESTRATOR (:6789)                        │
│                                                                     │
│  ┌──────────────────────────────────────────────────┐               │
│  │  PIPELINE 1: raw_ingestion                       │               │
│  │                                                  │               │
│  │  [download_ny_taxi_data]  ← Data Loader          │               │
│  │          │  Template Method + Command + Observer │               │
│  │          ▼                                       │               │
│  │  [transform_raw_minimal]  ← Transformer          │               │
│  │          │  Template Method (tipado básico)      │               │
│  │          ▼                                       │               │
│  │  [export_to_raw_postgres] ← Data Exporter        │               │
│  │             Template Method + State + Observer   │               │
│  └──────────────────────────────────────────────────┘               │
│                    │                                                │
│                    │  Evento: RAW_PIPELINE_COMPLETED (Observer)     │
│                    ▼                                                │
│  ┌──────────────────────────────────────────────────┐               │
│  │  PIPELINE 2: clean_transformation                │               │
│  │                                                  │               │
│  │  [load_from_raw_schema]   ← Data Loader          │               │
│  │          │  Template Method                      │               │
│  │          ▼                                       │               │
│  │  [transform_to_clean]     ← Transformer          │               │
│  │          │  Template Method + Command + Observer │               │
│  │          ▼                                       │               │
│  │  [export_to_clean_postgres] ← Data Exporter      │               │
│  │             Template Method + State + Observer   │               │
│  └──────────────────────────────────────────────────┘               │
│                                                                     │
│  Triggers:                                                          │
│    raw_ingestion_daily_2am   → @daily 02:00 UTC                     │
│    clean_after_raw_4am       → @daily 04:00 UTC                     │
└──────────────────────┬──────────────────────────────────────────────┘
                       │ psycopg2 / SQLAlchemy
                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    POSTGRESQL (:5432)                               │
│                                                                     │
│  DATABASE: nyc_taxi_db                                              │
│                                                                     │
│  SCHEMA: raw                        SCHEMA: clean                   │
│  ┌─────────────────────┐            ┌──────────────────────────┐    │
│  │ yellow_taxi_trips   │            │ dim_vendor               │    │
│  │ (tabla única plana) │            │ dim_payment_type         │    │
│  │                     │            │ dim_rate_code            │    │
│  │ Columnas ~25        │            │ dim_location             │    │
│  │ + metadata:         │            │ dim_date                 │    │
│  │   source_year       │            │                          │    │
│  │   source_month      │            │ fact_trips  ◄────────────┤    │
│  │   source_file       │            │ (FK → todas dims)        │    │
│  │   loaded_at         │            └──────────────────────────┘    │
│  └─────────────────────┘                                            │
└──────────────────────┬──────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    PGADMIN (:5050)                                  │
│           Inspección y validación visual de resultados              │
└─────────────────────────────────────────────────────────────────────┘
```

## Modelo Dimensional (Star Schema)

```
                    dim_vendor
                   ┌──────────────┐
                   │ vendor_key PK│
                   │ vendor_id    │
                   │ vendor_name  │
                   └──────┬───────┘
                          │
    dim_date              │            dim_payment_type
   ┌──────────────┐       │           ┌──────────────────┐
   │ date_key  PK │       │           │ payment_type_key │
   │ full_date    │       │           │ payment_type_id  │
   │ year/month   │       │           │ payment_type_name│
   │ day/quarter  │       │           └──────────┬───────┘
   │ is_weekend   │       │                      │
   └──────┬───────┘       │                      │
          │               │                      │
          │    ┌──────────▼──────────────────────▼───┐
          └────► fact_trips                           ◄────┐
               │ trip_id          PK (BIGSERIAL)      │    │
               │ vendor_key       FK→dim_vendor        │    │
               │ pickup_date_key  FK→dim_date          │    │
               │ dropoff_date_key FK→dim_date          │    │
               │ pickup_location_key  FK→dim_location  │    │
               │ dropoff_location_key FK→dim_location  │    │
               │ payment_type_key FK→dim_payment_type  │    │
               │ rate_code_key    FK→dim_rate_code      │    │
               │ ─── MÉTRICAS ───────────────────────  │    │
               │ passenger_count                       │    │
               │ trip_distance                         │    │
               │ fare_amount                           │    │
               │ extra, mta_tax, tip_amount            │    │
               │ tolls_amount, improvement_surcharge   │    │
               │ congestion_surcharge, airport_fee     │    │
               │ total_amount                          │    │
               │ trip_duration_minutes                 │    │
               │ source_year, source_month             │    │
               └───────────────────────────────────────┘    │
                          │                                  │
          dim_location    │             dim_rate_code        │
         ┌─────────────┐  │            ┌───────────────────┐ │
         │location_key │──┘            │ rate_code_key     │─┘
         │location_id  │               │ rate_code_id      │
         │borough      │               │ rate_code_name    │
         │zone         │               └───────────────────┘
         │service_zone │
         └─────────────┘
```

## Granularidad de fact_trips
**1 fila = 1 viaje de taxi amarillo en NYC**

## Decisiones de diseño

| Decisión | Justificación |
|---|---|
| Schema raw = tabla única | Preserva datos crudos sin pérdida, permite recargar clean desde raw |
| Schema clean = star schema | Permite análisis multidimensional eficiente con JOINs simples |
| location_id 0 = "Unknown" | Clave sustituta segura para datos con location_id inválido |
| rate_code_id 99 = "Unknown" | Misma razón, evita FK violations |
| Eliminar datos pre-2017 de clean | Pre-2017 usa lat/long, no LocationID; incompatible con dim_location |
| Idempotencia por source_file | Permite reejecutar pipelines sin duplicar datos |
| Template Method en bloques | Garantiza el flujo setup→validate→run→teardown en todos los bloques |
| Command con retry | Maneja fallos transitorios de red al descargar archivos TLC |
| Observer para eventos | Permite auditoría y encadenamiento de pipelines sin acoplamiento |
| State en exporters | Permite trazar estado de ejecución y recuperarse de fallos |

## Patrones de diseño aplicados

| Patrón | Archivo | Aplicación |
|---|---|---|
| Template Method | `utils/patterns/template.py` | Esqueleto de ejecución de todos los bloques Mage |
| Command | `utils/patterns/command.py` | Descarga parquet con retry/backoff, delete idempotente |
| Observer | `utils/patterns/observer.py` | Eventos de pipeline (RAW_FILE_LOADED, COMPLETED, FAILED) |
| State | `utils/patterns/state.py` | Tracking de estado: PENDIENTE→EN_EJECUCION→COMPLETADO/FALLIDO |
