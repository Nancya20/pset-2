"""
Patrón Template Method aplicado a bloques de Mage.

El Template Method define el ESQUELETO del proceso de ejecución de un bloque
(setup → validate_config → run → teardown) y deja que las subclases concretas
implementen los pasos variables (validate_config, run).

Referencia: notas-template.ipynb del curso.

Analogía con el curso:
  AbstractClass  → BasePipelineBlock
  template_method → execute()
  pasos variables → validate_config(), run()
  pasos fijos     → setup(), teardown(), _log_start(), _log_end()
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


class BasePipelineBlock(ABC):
    """
    Clase base abstracta para todos los bloques Mage del proyecto.
    Define el Template Method `execute()`.
    """

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        """
        Template Method: define el flujo fijo de ejecución.
        Las subclases NO deben sobrescribir este método.
        """
        start = datetime.now(timezone.utc)
        self._log_start()
        self.setup(kwargs)
        self.validate_config(kwargs)
        result = self.run(*args, **kwargs)
        self.teardown(kwargs)
        self._log_end(start)
        return result

    # ── Pasos fijos ──────────────────────────────────────────────
    def _log_start(self) -> None:
        logger.info("[%s] Iniciando bloque", self.__class__.__name__)

    def _log_end(self, start: datetime) -> None:
        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info("[%s] Bloque completado en %.2fs", self.__class__.__name__, elapsed)

    # ── Hooks opcionales (no abstractos) ─────────────────────────
    def setup(self, kwargs: dict) -> None:
        """Hook: preparación opcional antes de ejecutar."""
        pass

    def teardown(self, kwargs: dict) -> None:
        """Hook: limpieza opcional después de ejecutar."""
        pass

    # ── Pasos variables (abstractos, subclases los implementan) ──
    @abstractmethod
    def validate_config(self, kwargs: dict) -> None:
        """Valida que la configuración necesaria esté disponible."""
        ...

    @abstractmethod
    def run(self, *args: Any, **kwargs: Any) -> Any:
        """Lógica principal del bloque."""
        ...


class BaseDataLoaderBlock(BasePipelineBlock):
    """Template para bloques de tipo data_loader."""

    def validate_config(self, kwargs: dict) -> None:
        import os
        required = ["PG_USER", "PG_PASSWORD", "PG_HOST", "PG_PORT", "PG_DATABASE"]
        missing = [v for v in required if not os.environ.get(v)]
        if missing:
            raise EnvironmentError(f"Variables de entorno faltantes: {missing}")


class BaseTransformerBlock(BasePipelineBlock):
    """Template para bloques de tipo transformer."""

    def validate_config(self, kwargs: dict) -> None:
        pass  # Los transformers validan el DataFrame de entrada en run()


class BaseExporterBlock(BasePipelineBlock):
    """Template para bloques de tipo data_exporter."""

    def validate_config(self, kwargs: dict) -> None:
        import os
        required = ["PG_USER", "PG_PASSWORD", "PG_HOST", "PG_PORT", "PG_DATABASE"]
        missing = [v for v in required if not os.environ.get(v)]
        if missing:
            raise EnvironmentError(f"Variables de entorno faltantes: {missing}")
