"""
Pipeline Raw — Bloque 2: Transformer
Transformaciones MÍNIMAS para que la capa raw sea técnicamente cargable.

Regla: NO aplicar lógica de negocio aquí.
  ✓ Renombrado técnico (ya hecho en el loader, validamos aquí)
  ✓ Tipado básico para compatibilidad con PostgreSQL
  ✓ Eliminación de columnas completamente vacías
  ✗ Filtrado de registros por reglas de negocio (eso es trabajo de clean)
  ✗ Imputación de nulos con valores del dominio

Patrón aplicado: Template Method (BaseTransformerBlock).
"""
import os
import sys
import logging
import pandas as pd

sys.path.insert(0, "/home/src/ny_taxi_project")

if "transformer" not in dir():
    from mage_ai.data_preparation.decorators import transformer, test

from utils.patterns.template import BaseTransformerBlock

logger = logging.getLogger(__name__)

# Columnas de tipo datetime esperadas
DATETIME_COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

# Columnas numéricas que deben ser float
NUMERIC_COLS = [
    "trip_distance", "fare_amount", "extra", "mta_tax",
    "tip_amount", "tolls_amount", "improvement_surcharge",
    "congestion_surcharge", "airport_fee", "total_amount",
    "passenger_count",
    "pickup_longitude", "pickup_latitude",
    "dropoff_longitude", "dropoff_latitude",
]

# Columnas de tipo entero
INTEGER_COLS = ["vendorid", "payment_type", "pulocationid", "dolocationid"]


class RawMinimalTransformer(BaseTransformerBlock):
    """
    Aplica tipado técnico mínimo sin lógica de negocio.
    El Template Method (execute) se hereda de BasePipelineBlock.
    """

    def run(self, data: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
        if data is None or data.empty:
            raise ValueError("DataFrame de entrada está vacío o es None")

        df = data.copy()
        initial_cols = len(df.columns)

        df = self._cast_datetimes(df)
        df = self._cast_numerics(df)
        df = self._cast_integers(df)
        df = self._standardize_string_cols(df)
        df = self._drop_all_null_columns(df)

        logger.info(
            "[RawMinimalTransformer] %d registros | %d columnas (antes: %d)",
            len(df), len(df.columns), initial_cols
        )
        return df

    def _cast_datetimes(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in DATETIME_COLS:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=False)
                # Eliminar timezone si viene con tz-aware (PostgreSQL TIMESTAMP sin tz)
                if hasattr(df[col].dtype, "tz") and df[col].dtype.tz is not None:
                    df[col] = df[col].dt.tz_localize(None)
        return df

    def _cast_numerics(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in NUMERIC_COLS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    def _cast_integers(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in INTEGER_COLS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                # Mantener como float (permite NaN) para compatibilidad PG
                df[col] = df[col].astype("float64")
        return df

    def _standardize_string_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        if "store_and_fwd_flag" in df.columns:
            df["store_and_fwd_flag"] = (
                df["store_and_fwd_flag"]
                .astype(str)
                .str.strip()
                .str.upper()
                .replace({"NAN": None, "NONE": None, "": None})
            )
        return df

    def _drop_all_null_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        all_null = [c for c in df.columns if df[c].isna().all()]
        if all_null:
            logger.info("[RawMinimalTransformer] Eliminando columnas 100%% nulas: %s", all_null)
            df = df.drop(columns=all_null)
        return df


# ─────────────────────────────────────────────────────────────────
# Interfaz Mage
# ─────────────────────────────────────────────────────────────────
@transformer
def transform(data: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    t = RawMinimalTransformer()
    return t.execute(data, *args, **kwargs)


@test
def test_output(output, *args) -> None:
    assert output is not None, "Output no puede ser None"
    assert isinstance(output, pd.DataFrame), "Output debe ser DataFrame"
    assert "tpep_pickup_datetime" in output.columns, "Falta tpep_pickup_datetime"
    assert output["source_file"].notna().all(), "source_file no debe tener nulos"
