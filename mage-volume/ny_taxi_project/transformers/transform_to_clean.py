"""
Pipeline Clean — Bloque 2: Transformer
Limpieza, validación y construcción del modelo dimensional.

Proceso:
  1. Limpieza y validaciones de calidad de datos
  2. Construcción de tablas de dimensiones
  3. Construcción de la tabla de hechos con claves sustitutas

Modelo dimensional generado:
  clean.dim_vendor          → vendors del servicio de taxi
  clean.dim_payment_type    → tipos de pago
  clean.dim_rate_code       → tipos de tarifa
  clean.dim_location        → zonas de pickup/dropoff (de TLC zone lookup)
  clean.dim_date            → dimensión fecha
  clean.fact_trips          → tabla de hechos de viajes (granularidad: 1 fila = 1 viaje)

Patrones aplicados:
  - Template Method: BaseTransformerBlock
  - Command:         LoadZoneLookupCommand
  - Observer:        publica CLEAN_PIPELINE_STARTED
"""
import os
import sys
import logging
import pandas as pd
import numpy as np
from datetime import date

sys.path.insert(0, "/home/src/ny_taxi_project")

if "transformer" not in dir():
    from mage_ai.data_preparation.decorators import transformer, test

from utils.patterns.template import BaseTransformerBlock
from utils.patterns.command import LoadZoneLookupCommand, CommandRunner
from utils.patterns.observer import event_bus, PipelineEvent

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────
# Diccionarios de dimensiones estáticas
# ─────────────────────────────────────────────────────────────────
VENDOR_DATA = [
    {"vendor_key": 1, "vendor_id": 1, "vendor_name": "Creative Mobile Technologies"},
    {"vendor_key": 2, "vendor_id": 2, "vendor_name": "VeriFone Inc."},
    {"vendor_key": 0, "vendor_id": 0, "vendor_name": "Desconocido"},
]

PAYMENT_TYPE_DATA = [
    {"payment_type_key": 1, "payment_type_id": 1, "payment_type_name": "Credit card"},
    {"payment_type_key": 2, "payment_type_id": 2, "payment_type_name": "Cash"},
    {"payment_type_key": 3, "payment_type_id": 3, "payment_type_name": "No charge"},
    {"payment_type_key": 4, "payment_type_id": 4, "payment_type_name": "Dispute"},
    {"payment_type_key": 5, "payment_type_id": 5, "payment_type_name": "Unknown"},
    {"payment_type_key": 6, "payment_type_id": 6, "payment_type_name": "Voided trip"},
    {"payment_type_key": 0, "payment_type_id": 0, "payment_type_name": "Desconocido"},
]

RATE_CODE_DATA = [
    {"rate_code_key": 1, "rate_code_id": 1, "rate_code_name": "Standard rate"},
    {"rate_code_key": 2, "rate_code_id": 2, "rate_code_name": "JFK"},
    {"rate_code_key": 3, "rate_code_id": 3, "rate_code_name": "Newark"},
    {"rate_code_key": 4, "rate_code_id": 4, "rate_code_name": "Nassau or Westchester"},
    {"rate_code_key": 5, "rate_code_id": 5, "rate_code_name": "Negotiated fare"},
    {"rate_code_key": 6, "rate_code_id": 6, "rate_code_name": "Group ride"},
    {"rate_code_key": 99,"rate_code_id": 99,"rate_code_name": "Desconocido"},
]


class CleanDimensionalTransformer(BaseTransformerBlock):
    """
    Construye el modelo dimensional desde datos raw.
    Retorna un dict con un DataFrame por tabla del modelo.
    """

    def run(self, data: pd.DataFrame, *args, **kwargs) -> dict:
        event_bus.publish(PipelineEvent.CLEAN_PIPELINE_STARTED, {"input_records": len(data)})

        df = self._validate_and_clean(data)
        logger.info("[CleanTransformer] %d registros válidos (de %d raw)", len(df), len(data))

        # Dimensiones
        dim_vendor       = pd.DataFrame(VENDOR_DATA)
        dim_payment_type = pd.DataFrame(PAYMENT_TYPE_DATA)
        dim_rate_code    = pd.DataFrame(RATE_CODE_DATA)
        dim_location     = self._build_dim_location()
        dim_date         = self._build_dim_date(df)

        # Fact table
        fact_trips = self._build_fact_trips(df, dim_vendor, dim_payment_type, dim_rate_code, dim_location)

        logger.info("[CleanTransformer] fact_trips: %d filas", len(fact_trips))
        return {
            "dim_vendor":       dim_vendor,
            "dim_payment_type": dim_payment_type,
            "dim_rate_code":    dim_rate_code,
            "dim_location":     dim_location,
            "dim_date":         dim_date,
            "fact_trips":       fact_trips,
        }

    # ─── Validación y limpieza ────────────────────────────────────
    def _validate_and_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # 1. Timestamps válidos
        df = df[df["tpep_pickup_datetime"].notna() & df["tpep_dropoff_datetime"].notna()]
        df = df[df["tpep_dropoff_datetime"] > df["tpep_pickup_datetime"]]

        # 2. Fechas dentro del rango del dataset (2015-2025)
        df = df[df["tpep_pickup_datetime"] >= "2015-01-01"]
        df = df[df["tpep_pickup_datetime"] <  "2026-01-01"]

        # 3. Duración razonable (entre 1 minuto y 24 horas)
        duration = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds() / 60
        df = df[(duration >= 1) & (duration <= 1440)]

        # 4. Distancia razonable (entre 0.01 y 500 millas)
        df = df[df["trip_distance"].notna()]
        df = df[(df["trip_distance"] > 0) & (df["trip_distance"] < 500)]

        # 5. Monto total positivo
        df = df[df["total_amount"].notna()]
        df = df[df["total_amount"] >= 0]

        # 6. Fare amount positivo
        df = df[df["fare_amount"].notna()]
        df = df[df["fare_amount"] >= 0]

        # 7. LocationIDs válidos (1-265 según TLC)
        df = df[df["pulocationid"].notna() & df["dolocationid"].notna()]
        df = df[(df["pulocationid"].between(1, 265)) & (df["dolocationid"].between(1, 265))]

        # 8. Passenger count razonable (1-9)
        df["passenger_count"] = df["passenger_count"].clip(lower=1, upper=9)

        # 9. Eliminar duplicados exactos (mismo pickup, dropoff, distancia, monto)
        dup_cols = [
            "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "pulocationid", "dolocationid", "total_amount"
        ]
        before = len(df)
        df = df.drop_duplicates(subset=dup_cols)
        dropped = before - len(df)
        if dropped > 0:
            logger.info("[CleanTransformer] Eliminados %d duplicados", dropped)

        # 10. Imputar nulos en surcharges con 0
        for col in ["congestion_surcharge", "airport_fee", "improvement_surcharge",
                    "extra", "mta_tax", "tolls_amount", "tip_amount"]:
            if col in df.columns:
                df[col] = df[col].fillna(0.0)

        df = df.reset_index(drop=True)
        return df

    # ─── Dimensión Location ───────────────────────────────────────
    def _build_dim_location(self) -> pd.DataFrame:
        runner = CommandRunner(max_retries=3)
        cmd = LoadZoneLookupCommand()
        try:
            zones = runner.run(cmd)
            # Renombrar para el modelo
            zones = zones.rename(columns={
                "locationid": "location_id",
                "borough":    "borough",
                "zone":       "zone",
                "service_zone": "service_zone",
            })
            zones["location_key"] = zones["location_id"].astype(int)
            # Agregar fila "desconocido" para location_id=0
            unknown = pd.DataFrame([{
                "location_key": 0, "location_id": 0,
                "borough": "Unknown", "zone": "Unknown", "service_zone": "Unknown"
            }])
            return pd.concat([unknown, zones[["location_key", "location_id", "borough", "zone", "service_zone"]]], ignore_index=True)
        except Exception as exc:
            logger.warning("[CleanTransformer] No se pudo cargar zone lookup: %s. Usando placeholder.", exc)
            return pd.DataFrame([{
                "location_key": 0, "location_id": 0,
                "borough": "Unknown", "zone": "Unknown", "service_zone": "Unknown"
            }])

    # ─── Dimensión Date ───────────────────────────────────────────
    def _build_dim_date(self, df: pd.DataFrame) -> pd.DataFrame:
        dates = pd.date_range("2015-01-01", "2026-12-31", freq="D")
        return pd.DataFrame({
            "date_key":    dates.strftime("%Y%m%d").astype(int),
            "full_date":   dates.date,
            "year":        dates.year.astype("int16"),
            "month":       dates.month.astype("int16"),
            "day":         dates.day.astype("int16"),
            "quarter":     dates.quarter.astype("int16"),
            "day_of_week": dates.dayofweek.astype("int16"),  # 0=lunes
            "day_name":    dates.day_name(),
            "month_name":  dates.month_name(),
            "is_weekend":  dates.dayofweek.isin([5, 6]),
        })

    # ─── Tabla de Hechos ──────────────────────────────────────────
    def _build_fact_trips(
        self, df: pd.DataFrame,
        dim_vendor, dim_payment, dim_rate, dim_location
    ) -> pd.DataFrame:
        # Mapas para lookup de claves sustitutas
        vendor_map   = {int(r["vendor_id"]): int(r["vendor_key"])   for _, r in dim_vendor.iterrows()}
        payment_map  = {int(r["payment_type_id"]): int(r["payment_type_key"]) for _, r in dim_payment.iterrows()}
        rate_map     = {int(r["rate_code_id"]): int(r["rate_code_key"])   for _, r in dim_rate.iterrows()}
        location_map = {int(r["location_id"]): int(r["location_key"]) for _, r in dim_location.iterrows()}

        fact = pd.DataFrame()
        fact["tpep_pickup_datetime"]  = df["tpep_pickup_datetime"]
        fact["tpep_dropoff_datetime"] = df["tpep_dropoff_datetime"]

        # Claves sustitutas
        fact["vendor_key"] = (
            df["vendorid"].fillna(0).astype(int)
            .map(vendor_map).fillna(vendor_map.get(0, 0)).astype(int)
        )
        fact["pickup_date_key"] = (
            pd.to_datetime(df["tpep_pickup_datetime"]).dt.strftime("%Y%m%d").astype(int)
        )
        fact["dropoff_date_key"] = (
            pd.to_datetime(df["tpep_dropoff_datetime"]).dt.strftime("%Y%m%d").astype(int)
        )
        fact["pickup_location_key"] = (
            df["pulocationid"].fillna(0).astype(int)
            .map(location_map).fillna(location_map.get(0, 0)).astype(int)
        )
        fact["dropoff_location_key"] = (
            df["dolocationid"].fillna(0).astype(int)
            .map(location_map).fillna(location_map.get(0, 0)).astype(int)
        )
        fact["payment_type_key"] = (
            df["payment_type"].fillna(0).astype(int)
            .map(payment_map).fillna(payment_map.get(0, 0)).astype(int)
        )
        fact["rate_code_key"] = (
            df["ratecodeid"].fillna(99).astype(int)
            .map(rate_map).fillna(rate_map.get(99, 99)).astype(int)
        )

        # Métricas
        fact["passenger_count"]       = df["passenger_count"].fillna(1).clip(1, 9).astype("Int16")
        fact["trip_distance"]         = df["trip_distance"].round(2)
        fact["fare_amount"]           = df["fare_amount"].round(2)
        fact["extra"]                 = df["extra"].fillna(0).round(2)
        fact["mta_tax"]               = df["mta_tax"].fillna(0).round(2)
        fact["tip_amount"]            = df["tip_amount"].fillna(0).round(2)
        fact["tolls_amount"]          = df["tolls_amount"].fillna(0).round(2)
        fact["improvement_surcharge"] = df["improvement_surcharge"].fillna(0).round(2)
        fact["congestion_surcharge"]  = df["congestion_surcharge"].fillna(0).round(2)
        fact["airport_fee"]           = df.get("airport_fee", pd.Series(0.0, index=df.index)).fillna(0).round(2)
        fact["total_amount"]          = df["total_amount"].round(2)
        fact["trip_duration_minutes"] = (
            (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
            .dt.total_seconds() / 60
        ).round(2)
        fact["store_and_fwd_flag"] = df.get("store_and_fwd_flag", pd.Series(None, index=df.index))
        fact["source_year"]  = df["source_year"].astype("Int16")
        fact["source_month"] = df["source_month"].astype("Int16")

        return fact.reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────
# Interfaz Mage
# ─────────────────────────────────────────────────────────────────
@transformer
def transform(data: pd.DataFrame, *args, **kwargs) -> dict:
    t = CleanDimensionalTransformer()
    return t.execute(data, *args, **kwargs)


@test
def test_output(output, *args) -> None:
    assert isinstance(output, dict), "Output debe ser un dict"
    expected_keys = ["dim_vendor", "dim_payment_type", "dim_rate_code",
                     "dim_location", "dim_date", "fact_trips"]
    for k in expected_keys:
        assert k in output, f"Falta tabla: {k}"
    assert len(output["fact_trips"]) > 0, "fact_trips está vacía"
    assert "vendor_key" in output["fact_trips"].columns, "Falta vendor_key en fact_trips"
    assert "total_amount" in output["fact_trips"].columns, "Falta total_amount"
