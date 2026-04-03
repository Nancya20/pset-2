"""
Pipeline Raw — Bloque 1: Data Loader
Descarga archivos parquet de NY Yellow Taxi desde el CDN de TLC.

Patrones aplicados (ver notebooks del curso):
  - Template Method: hereda de BaseDataLoaderBlock (template.py)
  - Command:         DownloadParquetCommand con retry (command.py)
  - Observer:        publica eventos RAW_FILE_LOADED / RAW_FILE_FAILED (observer.py)
"""
import os
import sys
import logging
import pandas as pd

sys.path.insert(0, "/home/src/ny_taxi_project")

if "data_loader" not in dir():
    from mage_ai.data_preparation.decorators import data_loader, test

from utils.patterns.template import BaseDataLoaderBlock
from utils.patterns.command import DownloadParquetCommand, CommandRunner
from utils.patterns.observer import event_bus, PipelineEvent

logger = logging.getLogger(__name__)

STANDARD_COLUMNS = {
    "VendorID":              "vendorid",
    "RatecodeID":            "ratecodeid",
    "PULocationID":          "pulocationid",
    "DOLocationID":          "dolocationid",
    "tpep_pickup_datetime":  "tpep_pickup_datetime",
    "tpep_dropoff_datetime": "tpep_dropoff_datetime",
    "passenger_count":       "passenger_count",
    "trip_distance":         "trip_distance",
    "store_and_fwd_flag":    "store_and_fwd_flag",
    "payment_type":          "payment_type",
    "fare_amount":           "fare_amount",
    "extra":                 "extra",
    "mta_tax":               "mta_tax",
    "tip_amount":            "tip_amount",
    "tolls_amount":          "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "congestion_surcharge":  "congestion_surcharge",
    "airport_fee":           "airport_fee",
    "total_amount":          "total_amount",
    "pickup_longitude":      "pickup_longitude",
    "pickup_latitude":       "pickup_latitude",
    "dropoff_longitude":     "dropoff_longitude",
    "dropoff_latitude":      "dropoff_latitude",
}


class NYTaxiRawLoader(BaseDataLoaderBlock):

    def run(self, *args, **kwargs) -> pd.DataFrame:
        start_year  = int(os.environ.get("START_YEAR", 2024))
        end_year    = int(os.environ.get("END_YEAR", 2024))
        start_month = int(os.environ.get("START_MONTH", 1))
        end_month   = int(os.environ.get("END_MONTH", 1))

        runner = CommandRunner(max_retries=3, base_delay=2.0)
        all_frames = []

        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                if year == start_year and month < start_month:
                    continue
                if year == end_year and month > end_month:
                    continue

                source_file = f"yellow_tripdata_{year}-{month:02d}.parquet"
                cmd = DownloadParquetCommand(year=year, month=month)

                try:
                    df = runner.run(cmd)
                    df = self._add_metadata(df, year, month, source_file)
                    all_frames.append(df)
                    event_bus.publish(PipelineEvent.RAW_FILE_LOADED, {
                        "source_file": source_file,
                        "records": len(df),
                        "year": year,
                        "month": month,
                    })
                except Exception as exc:
                    logger.error("Fallo cargando %s: %s", source_file, exc)
                    event_bus.publish(PipelineEvent.RAW_FILE_FAILED, {
                        "source_file": source_file,
                        "error": str(exc),
                    })

        if not all_frames:
            raise RuntimeError("No se pudo descargar ningún archivo.")

        combined = pd.concat(all_frames, ignore_index=True)
        logger.info("Total registros descargados: %d", len(combined))
        return combined

    def _add_metadata(self, df, year, month, source_file):
        rename_map = {c: STANDARD_COLUMNS[c] for c in df.columns if c in STANDARD_COLUMNS}
        df = df.rename(columns=rename_map)
        df.columns = [c.lower() for c in df.columns]
        df["source_year"]  = year
        df["source_month"] = month
        df["source_file"]  = source_file
        return df


@data_loader
def load_data(*args, **kwargs) -> pd.DataFrame:
    loader = NYTaxiRawLoader()
    return loader.execute(*args, **kwargs)


@test
def test_output(output, *args) -> None:
    assert output is not None, "El output no puede ser None"
    assert isinstance(output, pd.DataFrame), "El output debe ser un DataFrame"
    assert len(output) > 0, "El DataFrame no puede estar vacío"
    assert "source_file" in output.columns, "Falta columna source_file"
    assert "source_year" in output.columns, "Falta columna source_year"
    logger.info("[test] %d registros, %d columnas", len(output), len(output.columns))
