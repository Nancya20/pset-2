"""
Patrón Command aplicado a operaciones del pipeline ELT.

Cada operación de carga/transformación se encapsula como un Command:
- permite retries con backoff
- permite logging/auditoría de cada operación
- permite deshacer (compensating action) en caso de fallo
- facilita idempotencia mediante command_id

Referencia: notas-command.ipynb del curso.

Analogía con el curso:
  Command         → PipelineCommand
  ConcreteCommand → DownloadParquetCommand, LoadRawCommand, UpsertCleanCommand
  Invoker         → CommandRunner (con retry/backoff)
  Receiver        → PostgreSQL, TLC CDN HTTP
"""
from __future__ import annotations

import logging
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import requests

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# Interfaz base
# ─────────────────────────────────────────────────────────────────
class PipelineCommand(ABC):
    """
    Interfaz base para todos los comandos del pipeline.
    Cada comando es idempotente gracias a su command_id.
    """

    def __init__(self) -> None:
        self.command_id: str = str(uuid.uuid4())

    @abstractmethod
    def execute(self) -> Any:
        """Ejecuta el comando; debe ser idempotente."""
        ...

    def undo(self) -> None:
        """Acción compensatoria en caso de fallo (compensating action)."""
        logger.warning("[%s] undo() no implementado", self.__class__.__name__)


# ─────────────────────────────────────────────────────────────────
# Invoker con retry/backoff
# ─────────────────────────────────────────────────────────────────
@dataclass
class CommandRunner:
    """
    Invoker: ejecuta un comando con política de reintentos exponenciales.
    El invoker no conoce la implementación concreta del comando.
    """
    max_retries: int = 3
    base_delay: float = 2.0   # segundos

    def run(self, command: PipelineCommand) -> Any:
        attempt = 0
        last_error: Exception | None = None

        while attempt <= self.max_retries:
            try:
                logger.info(
                    "[CommandRunner] Ejecutando %s (intento %d/%d, id=%s)",
                    command.__class__.__name__, attempt + 1,
                    self.max_retries + 1, command.command_id
                )
                result = command.execute()
                logger.info("[CommandRunner] %s completado OK", command.__class__.__name__)
                return result
            except Exception as exc:
                last_error = exc
                attempt += 1
                if attempt <= self.max_retries:
                    delay = self.base_delay ** attempt
                    logger.warning(
                        "[CommandRunner] Fallo en %s: %s. Reintentando en %.1fs...",
                        command.__class__.__name__, exc, delay
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        "[CommandRunner] %s falló después de %d intentos: %s",
                        command.__class__.__name__, self.max_retries + 1, exc
                    )
                    try:
                        command.undo()
                    except Exception:
                        pass
                    raise last_error


# ─────────────────────────────────────────────────────────────────
# Comandos concretos
# ─────────────────────────────────────────────────────────────────
@dataclass
class DownloadParquetCommand(PipelineCommand):
    """
    Descarga un archivo parquet de NY TLC y lo retorna como DataFrame.
    Idempotente: si el archivo ya fue descargado se puede saltear.
    """
    year: int
    month: int
    base_url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"

    def __post_init__(self):
        super().__init__()
        self.command_id = f"download_{self.year}_{self.month:02d}"

    def execute(self):
        import io
        import pandas as pd

        url = self.base_url.format(year=self.year, month=self.month)
        logger.info("[DownloadParquetCommand] Descargando %s", url)
        response = requests.get(url, timeout=300)
        response.raise_for_status()
        df = pd.read_parquet(io.BytesIO(response.content))
        logger.info(
            "[DownloadParquetCommand] %d registros descargados para %d-%02d",
            len(df), self.year, self.month
        )
        return df

    def undo(self):
        logger.info("[DownloadParquetCommand] undo: nada que revertir (descarga en memoria)")


@dataclass
class DeleteRawFileCommand(PipelineCommand):
    """
    Elimina registros de raw para un source_file dado (compensating action).
    Se usa para garantizar idempotencia antes de recargar.
    """
    engine: Any
    source_file: str

    def __post_init__(self):
        super().__init__()
        self.command_id = f"delete_raw_{self.source_file}"

    def execute(self) -> int:
        from sqlalchemy import text
        with self.engine.begin() as conn:
            result = conn.execute(
                text("DELETE FROM raw.yellow_taxi_trips WHERE source_file = :sf"),
                {"sf": self.source_file}
            )
            deleted = result.rowcount
        logger.info("[DeleteRawFileCommand] Eliminados %d registros de %s", deleted, self.source_file)
        return deleted


@dataclass
class LoadZoneLookupCommand(PipelineCommand):
    """Descarga y retorna el lookup de zonas de taxi de NYC."""
    url: str = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"

    def __post_init__(self):
        super().__init__()
        self.command_id = "load_zone_lookup"

    def execute(self):
        import pandas as pd
        logger.info("[LoadZoneLookupCommand] Descargando zona lookup de TLC")
        df = pd.read_csv(self.url)
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]
        return df
