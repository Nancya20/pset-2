"""
Pipeline Clean — Bloque 3: Data Exporter
Escribe el modelo dimensional en PostgreSQL schema clean.

Tablas creadas/actualizadas:
  clean.dim_vendor
  clean.dim_payment_type
  clean.dim_rate_code
  clean.dim_location
  clean.dim_date
  clean.fact_trips (con claves foráneas)

Idempotencia:
  - Dimensiones: UPSERT (INSERT ... ON CONFLICT DO NOTHING)
  - fact_trips:  DELETE WHERE (source_year, source_month) + INSERT

Patrones aplicados:
  - Template Method: BaseExporterBlock
  - State:           PipelineRunContext (seguimiento de estado)
  - Observer:        publica CLEAN_PIPELINE_COMPLETED
"""
import os
import sys
import logging
import uuid
import pandas as pd
from sqlalchemy import text

sys.path.insert(0, "/home/src/ny_taxi_project")

if "data_exporter" not in dir():
    from mage_ai.data_preparation.decorators import data_exporter

from utils.patterns.template import BaseExporterBlock
from utils.patterns.observer import event_bus, PipelineEvent
from utils.patterns.state import PipelineRunContext
from utils.db_helpers import get_engine

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────
# DDL del modelo dimensional
# ─────────────────────────────────────────────────────────────────
SCHEMA_DDL = "CREATE SCHEMA IF NOT EXISTS clean;"

DIMENSIONS_DDL = """
CREATE TABLE IF NOT EXISTS clean.dim_vendor (
    vendor_key   INTEGER PRIMARY KEY,
    vendor_id    INTEGER UNIQUE NOT NULL,
    vendor_name  VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS clean.dim_payment_type (
    payment_type_key  INTEGER PRIMARY KEY,
    payment_type_id   INTEGER UNIQUE NOT NULL,
    payment_type_name VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS clean.dim_rate_code (
    rate_code_key  INTEGER PRIMARY KEY,
    rate_code_id   INTEGER UNIQUE NOT NULL,
    rate_code_name VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS clean.dim_location (
    location_key  INTEGER PRIMARY KEY,
    location_id   INTEGER UNIQUE NOT NULL,
    borough       VARCHAR(50),
    zone          VARCHAR(100),
    service_zone  VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS clean.dim_date (
    date_key     INTEGER PRIMARY KEY,
    full_date    DATE,
    year         SMALLINT,
    month        SMALLINT,
    day          SMALLINT,
    quarter      SMALLINT,
    day_of_week  SMALLINT,
    day_name     VARCHAR(15),
    month_name   VARCHAR(15),
    is_weekend   BOOLEAN
);

CREATE TABLE IF NOT EXISTS clean.fact_trips (
    trip_id                BIGSERIAL PRIMARY KEY,
    vendor_key             INTEGER REFERENCES clean.dim_vendor(vendor_key),
    pickup_date_key        INTEGER REFERENCES clean.dim_date(date_key),
    dropoff_date_key       INTEGER REFERENCES clean.dim_date(date_key),
    pickup_location_key    INTEGER REFERENCES clean.dim_location(location_key),
    dropoff_location_key   INTEGER REFERENCES clean.dim_location(location_key),
    payment_type_key       INTEGER REFERENCES clean.dim_payment_type(payment_type_key),
    rate_code_key          INTEGER REFERENCES clean.dim_rate_code(rate_code_key),
    tpep_pickup_datetime   TIMESTAMP,
    tpep_dropoff_datetime  TIMESTAMP,
    passenger_count        SMALLINT,
    trip_distance          NUMERIC(10,2),
    fare_amount            NUMERIC(10,2),
    extra                  NUMERIC(10,2),
    mta_tax                NUMERIC(10,2),
    tip_amount             NUMERIC(10,2),
    tolls_amount           NUMERIC(10,2),
    improvement_surcharge  NUMERIC(10,2),
    congestion_surcharge   NUMERIC(10,2),
    airport_fee            NUMERIC(10,2),
    total_amount           NUMERIC(10,2),
    trip_duration_minutes  NUMERIC(10,2),
    store_and_fwd_flag     CHAR(1),
    source_year            SMALLINT,
    source_month           SMALLINT
);

CREATE INDEX IF NOT EXISTS idx_fact_pickup_date     ON clean.fact_trips(pickup_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_vendor          ON clean.fact_trips(vendor_key);
CREATE INDEX IF NOT EXISTS idx_fact_pickup_loc      ON clean.fact_trips(pickup_location_key);
CREATE INDEX IF NOT EXISTS idx_fact_source          ON clean.fact_trips(source_year, source_month);
CREATE INDEX IF NOT EXISTS idx_fact_pickup_datetime ON clean.fact_trips(tpep_pickup_datetime);
"""


class CleanPostgresExporter(BaseExporterBlock):

    def run(self, data: dict, *args, **kwargs) -> dict:
        engine = get_engine()
        run_ctx = PipelineRunContext(
            run_id=str(uuid.uuid4()),
            pipeline_name="clean_transformation"
        )
        run_ctx.start()

        try:
            # 1. Crear schema y tablas
            self._ensure_schema(engine)

            # 2. Cargar dimensiones (small tables, full upsert)
            self._upsert_dimension(engine, data["dim_vendor"], "clean.dim_vendor", "vendor_key")
            self._upsert_dimension(engine, data["dim_payment_type"], "clean.dim_payment_type", "payment_type_key")
            self._upsert_dimension(engine, data["dim_rate_code"], "clean.dim_rate_code", "rate_code_key")
            self._upsert_dimension(engine, data["dim_location"], "clean.dim_location", "location_key")
            self._upsert_dim_date(engine, data["dim_date"])

            # 3. Cargar fact_trips (idempotente por source_year/month)
            total = self._load_fact_trips(engine, data["fact_trips"])

            run_ctx.complete(total)
            event_bus.publish(PipelineEvent.CLEAN_PIPELINE_COMPLETED, {
                "fact_trips_loaded": total,
            })
            return run_ctx.to_dict()

        except Exception as exc:
            run_ctx.fail(exc)
            raise

    def _ensure_schema(self, engine) -> None:
        with engine.begin() as conn:
            conn.execute(text(SCHEMA_DDL))
            for stmt in DIMENSIONS_DDL.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.execute(text(stmt))
        logger.info("[CleanExporter] Schema clean y tablas verificadas")

    def _upsert_dimension(self, engine, df: pd.DataFrame, table: str, pk: str) -> None:
        # Estrategia: truncar y reinsertar (dimensiones son pequeñas y estáticas)
        schema, tbl = table.split(".")
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table} CASCADE"))
        df.to_sql(tbl, engine, schema=schema, if_exists="append", index=False, method="multi")
        logger.info("[CleanExporter] %s: %d filas", table, len(df))

    def _upsert_dim_date(self, engine, df: pd.DataFrame) -> None:
        # dim_date: insertar solo si no existe (es un rango fijo 2015-2026)
        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM clean.dim_date")).scalar()
        if count == 0:
            df.to_sql("dim_date", engine, schema="clean", if_exists="append", index=False,
                      method="multi", chunksize=1000)
            logger.info("[CleanExporter] dim_date: %d filas insertadas", len(df))
        else:
            logger.info("[CleanExporter] dim_date ya tiene %d filas, saltando", count)

    def _load_fact_trips(self, engine, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        # Idempotencia: borrar y reinsertar por combinación source_year/source_month
        for (year, month), group in df.groupby(["source_year", "source_month"]):
            with engine.begin() as conn:
                conn.execute(text("""
                    DELETE FROM clean.fact_trips
                    WHERE source_year = :y AND source_month = :m
                """), {"y": int(year), "m": int(month)})

            # Preparar dtypes para PostgreSQL
            group = group.copy()
            group["passenger_count"] = group["passenger_count"].astype(object).where(
                group["passenger_count"].notna(), None
            )
            group.to_sql(
                "fact_trips", engine, schema="clean",
                if_exists="append", index=False,
                method="multi", chunksize=5000
            )
            logger.info("[CleanExporter] fact_trips %d-%02d: %d filas", int(year), int(month), len(group))

        return len(df)


# ─────────────────────────────────────────────────────────────────
# Interfaz Mage
# ─────────────────────────────────────────────────────────────────
@data_exporter
def export_data(data: dict, *args, **kwargs) -> None:
    exporter = CleanPostgresExporter()
    result = exporter.execute(data, *args, **kwargs)
    logger.info("[export_to_clean_postgres] Resultado: %s", result)
