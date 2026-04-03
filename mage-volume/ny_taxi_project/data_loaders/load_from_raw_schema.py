"""
Pipeline Clean — Bloque 1: Data Loader
Lee datos desde raw.yellow_taxi_trips para el proceso de transformación.

Filtra datos con LocationID (2017+) que son los compatibles con el
modelo dimensional basado en zonas de taxi de NYC.

Patrón aplicado: Template Method (BaseDataLoaderBlock).
"""
import os
import sys
import logging
import pandas as pd
from sqlalchemy import text

sys.path.insert(0, "/home/src/ny_taxi_project")

if "data_loader" not in dir():
    from mage_ai.data_preparation.decorators import data_loader, test

from utils.patterns.template import BaseDataLoaderBlock
from utils.db_helpers import get_engine

logger = logging.getLogger(__name__)

QUERY = """
SELECT
    id,
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    pulocationid,
    dolocationid,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    total_amount,
    source_year,
    source_month,
    source_file
FROM raw.yellow_taxi_trips
WHERE
    -- Solo datos con LocationID (2017+): compatibles con modelo dimensional
    pulocationid IS NOT NULL
    AND dolocationid IS NOT NULL
    AND source_year >= 2017
    -- Filtro configurado por rango de fecha
    AND source_year  BETWEEN :start_year  AND :end_year
    AND source_month BETWEEN :start_month AND :end_month
"""


class RawSchemaLoader(BaseDataLoaderBlock):

    def run(self, *args, **kwargs) -> pd.DataFrame:
        engine = get_engine()
        start_year  = int(os.environ.get("START_YEAR", 2024))
        end_year    = int(os.environ.get("END_YEAR", 2024))
        start_month = int(os.environ.get("START_MONTH", 1))
        end_month   = int(os.environ.get("END_MONTH", 3))

        params = {
            "start_year": start_year, "end_year": end_year,
            "start_month": start_month, "end_month": end_month,
        }

        logger.info(
            "[RawSchemaLoader] Leyendo raw %d-%02d a %d-%02d",
            start_year, start_month, end_year, end_month
        )

        df = pd.read_sql(text(QUERY), engine, params=params)
        logger.info("[RawSchemaLoader] %d registros leídos de raw", len(df))
        return df


@data_loader
def load_data(*args, **kwargs) -> pd.DataFrame:
    loader = RawSchemaLoader()
    return loader.execute(*args, **kwargs)


@test
def test_output(output, *args) -> None:
    assert output is not None, "Output es None"
    assert len(output) > 0, "No hay datos en raw para el rango configurado"
    assert "pulocationid" in output.columns, "Falta pulocationid"
    assert "dolocationid" in output.columns, "Falta dolocationid"
