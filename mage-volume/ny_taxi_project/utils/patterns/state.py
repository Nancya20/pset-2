"""
Patrón State aplicado al seguimiento de estado de ejecución del pipeline.

Cada ejecución de pipeline transita entre estados:
  PENDIENTE → EN_EJECUCION → COMPLETADO
                          ↘ FALLIDO

El contexto (PipelineRunContext) delega el comportamiento al estado actual.
Cada estado controla qué transiciones son válidas.

Referencia: notas-state.ipynb del curso.

Analogía con el curso:
  Context         → PipelineRunContext
  State (ABC)     → PipelineRunState
  EstadoRojo etc. → PendingState, RunningState, SucceededState, FailedState
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# Interfaz de estado (equivalente a EstadoSemaforo del curso)
# ─────────────────────────────────────────────────────────────────
class PipelineRunState(ABC):

    @abstractmethod
    def start(self, ctx: "PipelineRunContext") -> None: ...

    @abstractmethod
    def complete(self, ctx: "PipelineRunContext", records: int) -> None: ...

    @abstractmethod
    def fail(self, ctx: "PipelineRunContext", error: Exception) -> None: ...

    @abstractmethod
    def status(self) -> str: ...


# ─────────────────────────────────────────────────────────────────
# Estados concretos
# ─────────────────────────────────────────────────────────────────
class PendingState(PipelineRunState):

    def start(self, ctx: "PipelineRunContext") -> None:
        ctx.started_at = datetime.now(timezone.utc)
        ctx.set_state(RunningState())
        logger.info("[PipelineRun:%s] PENDIENTE → EN_EJECUCION", ctx.run_id)

    def complete(self, ctx, records):
        raise InvalidTransitionError("No se puede completar un pipeline que no ha iniciado")

    def fail(self, ctx, error):
        raise InvalidTransitionError("No se puede fallar un pipeline que no ha iniciado")

    def status(self) -> str:
        return "PENDIENTE"


class RunningState(PipelineRunState):

    def start(self, ctx):
        raise InvalidTransitionError("El pipeline ya está en ejecución")

    def complete(self, ctx: "PipelineRunContext", records: int) -> None:
        ctx.finished_at = datetime.now(timezone.utc)
        ctx.records_processed = records
        ctx.set_state(SucceededState())
        logger.info(
            "[PipelineRun:%s] EN_EJECUCION → COMPLETADO | %d registros | %.2fs",
            ctx.run_id, records,
            (ctx.finished_at - ctx.started_at).total_seconds()
        )

    def fail(self, ctx: "PipelineRunContext", error: Exception) -> None:
        ctx.finished_at = datetime.now(timezone.utc)
        ctx.error = str(error)
        ctx.set_state(FailedState())
        logger.error(
            "[PipelineRun:%s] EN_EJECUCION → FALLIDO | error=%s",
            ctx.run_id, error
        )

    def status(self) -> str:
        return "EN_EJECUCION"


class SucceededState(PipelineRunState):

    def start(self, ctx):
        raise InvalidTransitionError("Pipeline ya completado")

    def complete(self, ctx, records):
        raise InvalidTransitionError("Pipeline ya completado")

    def fail(self, ctx, error):
        raise InvalidTransitionError("Pipeline ya completado")

    def status(self) -> str:
        return "COMPLETADO"


class FailedState(PipelineRunState):

    def start(self, ctx: "PipelineRunContext") -> None:
        # Permite reintentar desde un estado fallido
        ctx.error = None
        ctx.started_at = datetime.now(timezone.utc)
        ctx.set_state(RunningState())
        logger.info("[PipelineRun:%s] FALLIDO → EN_EJECUCION (reintento)", ctx.run_id)

    def complete(self, ctx, records):
        raise InvalidTransitionError("Pipeline está en estado fallido")

    def fail(self, ctx, error):
        ctx.error = str(error)
        logger.error("[PipelineRun:%s] Fallo adicional: %s", ctx.run_id, error)

    def status(self) -> str:
        return "FALLIDO"


# ─────────────────────────────────────────────────────────────────
# Contexto
# ─────────────────────────────────────────────────────────────────
class PipelineRunContext:
    """
    Contexto que mantiene el estado actual y delega comportamiento.
    Equivalente a LuzSemaforo del curso.
    """

    def __init__(self, run_id: str, pipeline_name: str) -> None:
        self.run_id = run_id
        self.pipeline_name = pipeline_name
        self._state: PipelineRunState = PendingState()
        self.started_at: Optional[datetime] = None
        self.finished_at: Optional[datetime] = None
        self.records_processed: int = 0
        self.error: Optional[str] = None

    def set_state(self, state: PipelineRunState) -> None:
        self._state = state

    # Delegación al estado actual
    def start(self) -> None:
        self._state.start(self)

    def complete(self, records: int = 0) -> None:
        self._state.complete(self, records)

    def fail(self, error: Exception) -> None:
        self._state.fail(self, error)

    @property
    def status(self) -> str:
        return self._state.status()

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "pipeline": self.pipeline_name,
            "status": self.status,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "records_processed": self.records_processed,
            "error": self.error,
        }


class InvalidTransitionError(Exception):
    """Transición de estado inválida."""
    pass
