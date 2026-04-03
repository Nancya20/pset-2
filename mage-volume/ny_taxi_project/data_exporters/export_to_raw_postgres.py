"""
Pipeline Raw — Bloque 3: Data Exporter
Carga el DataFrame transformado a PostgreSQL en el schema raw.

Características:
  - Crea el schema raw y la tabla si no existen
  - Idempotente: para cada source_file, elimina registros previos antes de insertar
    (Command DeleteRawFileCommand actúa como compensating action)
  - Publica evento RAW_PIPELINE_COMPLETED al finalizar (Observer)
  - Gestiona estado de ejecución (State pattern)

Patrones aplicados:
  - Template Method: BaseExporterBlock
  - Command:         DeleteRawFileCommand (idempotencia)
  - State:           PipelineRunContext (seguimiento de estado)
  - Observer:        event_bus.publish (notificación de completion)
"""
import os
import sys
import logging
import uuid
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import text

sys.path.insert(0, "/home/src/ny_taxi_project")

if "data_exporter" not in dir():
    from mage_ai.data_preparation.decorators import data_exporter

from utils.patterns.template import BaseExporterBlock
from utils.patterns.command import DeleteRawFileCommand, CommandRunner
from utils.patterns.observer import event_bus, PipelineEvent, PipelineTriggerObserver
from utils.patterns.state import PipelineRunContext
from utils.db_helpers import get_engine

logger = logging.getLogger(__name__)

RAW_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS raw.yellow_taxi_trips (
    id                     BIGSERIAL PRIMARY KEY,
    vendorid               FLOAT,
    tpep_pickup_datetime   TIMESTAMP,
    tpep_dropoff_datetime  TIMESTAMP,
    passenger_count        FLOAT,
    trip_distance          FLOAT,
    ratecodeid             FLOAT,
    store_and_fwd_flag     VARCHAR(3),
    pulocationid           FLOAT,
    dolocationid           FLOAT,
    pickup_longitude       FLOAT,
    pickup_latitude        FLOAT,
    dropoff_longitude      FLOAT,
    dropoff_latitude       FLOAT,
    payment_type           FLOAT,
    fare_amount            FLOAT,
    extra                  FLOAT,
    mta_tax                FLOAT,
    tip_amount             FLOAT,
    tolls_amount           FLOAT,
    improvement_surcharge  FLOAT,
    congestion_surcharge   FLOAT,
    airport_fee            FLOAT,
    total_amount           FLOAT,
    source_year            SMALLINT,
    source_month           SMALLINT,
    source_file            VARCHAR(100),
    loaded_at              TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_raw_source_file
    ON raw.yellow_taxi_trips(source_file);
CREATE INDEX IF NOT EXISTS idx_raw_pickup_datetime
    ON raw.yellow_taxi_trips(tpep_pickup_datetime);
"""

# Columnas del DataFrame que corresponden al schema de la tabla
TABLE_COLUMNS = [
    "vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "ratecodeid", "store_and_fwd_flag",
    "pulocationid", "dolocationid",
    "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude",
    "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge", "congestion_surcharge",
    "airport_fee", "total_amount",
    "source_year", "source_month", "source_file",
]


class RawPostgresExporter(BaseExporterBlock):
    """
    Exporta datos a la capa raw con operaciones idempotentes.
    """

    def run(self, data: pd.DataFrame, *args, **kwargs) -> dict:
        engine = get_engine()
        run_ctx = PipelineRunContext(
            run_id=str(uuid.uuid4()),
            pipeline_name="raw_ingestion"
        )
        # Suscribir el trigger observer (publica evento al completar)
        event_bus.subscribe(
            PipelineEvent.RAW_PIPELINE_COMPLETED,
            PipelineTriggerObserver(engine=engine)
        )

        run_ctx.start()
        try:
            # 1. Crear schema y tabla
            self._ensure_schema_and_table(engine)

            # 2. Preparar DataFrame con solo las columnas del schema
            df = self._prepare_dataframe(data)

            # 3. Carga por source_file (idempotente)
            total_records = 0
            runner = CommandRunner(max_retries=2)
            force_reload = os.environ.get("FORCE_RELOAD", "false").lower() == "true"

            for source_file, group in df.groupby("source_file"):
                already_exists = self._check_exists(engine, source_file)
                if already_exists and not force_reload:
                    logger.info("[RawExporter] Saltando %s (ya cargado)", source_file)
                    continue

                if already_exists and force_reload:
                    runner.run(DeleteRawFileCommand(engine=engine, source_file=source_file))

                group["loaded_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
                group.to_sql(
                    "yellow_taxi_trips",
                    engine,
                    schema="raw",
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=5000,
                )
                total_records += len(group)
                logger.info("[RawExporter] Cargados %d registros de %s", len(group), source_file)

            run_ctx.complete(total_records)
            event_bus.publish(PipelineEvent.RAW_PIPELINE_COMPLETED, {
                "records_loaded": total_records,
                "files_processed": df["source_file"].nunique(),
            })
            return run_ctx.to_dict()

        except Exception as exc:
            run_ctx.fail(exc)
            raise

    def _ensure_schema_and_table(self, engine) -> None:
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
            for stmt in RAW_TABLE_DDL.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.execute(text(stmt))
        logger.info("[RawExporter] Schema raw y tabla verificados")

    def _prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        present = [c for c in TABLE_COLUMNS if c in df.columns]
        missing = [c for c in TABLE_COLUMNS if c not in df.columns]
        result = df[present].copy()
        for col in missing:
            result[col] = None
        return result[TABLE_COLUMNS]

    def _check_exists(self, engine, source_file: str) -> bool:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT 1 FROM raw.yellow_taxi_trips WHERE source_file = :sf LIMIT 1"),
                {"sf": source_file}
            ).fetchone()
        return row is not None


# ─────────────────────────────────────────────────────────────────
# Interfaz Mage
# ─────────────────────────────────────────────────────────────────
@data_exporter
def export_data(data: pd.DataFrame, *args, **kwargs) -> None:
    exporter = RawPostgresExporter()
    result = exporter.execute(data, *args, **kwargs)
    logger.info("[export_to_raw_postgres] Resultado: %s", result)
