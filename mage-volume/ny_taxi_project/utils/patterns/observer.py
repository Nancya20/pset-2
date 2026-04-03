"""
Patrón Observer aplicado a eventos del pipeline ELT.

El pipeline publica eventos (RAW_FILE_LOADED, RAW_PIPELINE_COMPLETED,
CLEAN_PIPELINE_COMPLETED) y observadores reaccionan independientemente:
logging de auditoría, métricas, o disparo de la siguiente etapa.

Referencia: notas-pubsub.ipynb del curso.

Analogía con el curso:
  Publisher/Subject → PipelineEventBus
  Observer          → PipelineObserver (interfaz)
  ConcreteObserver  → AuditLogObserver, MetricsObserver, PipelineTriggerObserver
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# Tipos de eventos del pipeline
# ─────────────────────────────────────────────────────────────────
class PipelineEvent:
    RAW_FILE_LOADED          = "RAW_FILE_LOADED"
    RAW_FILE_SKIPPED         = "RAW_FILE_SKIPPED"
    RAW_FILE_FAILED          = "RAW_FILE_FAILED"
    RAW_PIPELINE_COMPLETED   = "RAW_PIPELINE_COMPLETED"
    CLEAN_PIPELINE_STARTED   = "CLEAN_PIPELINE_STARTED"
    CLEAN_PIPELINE_COMPLETED = "CLEAN_PIPELINE_COMPLETED"
    VALIDATION_FAILED        = "VALIDATION_FAILED"


# ─────────────────────────────────────────────────────────────────
# Interfaz Observer
# ─────────────────────────────────────────────────────────────────
class PipelineObserver(ABC):
    """Interfaz base para todos los observadores del pipeline."""

    @abstractmethod
    def update(self, event_type: str, payload: dict) -> None:
        """Reacciona al evento publicado."""
        ...


# ─────────────────────────────────────────────────────────────────
# Publisher / Event Bus
# ─────────────────────────────────────────────────────────────────
class PipelineEventBus:
    """
    Bus central de eventos del pipeline.
    Desacopla a los emisores de los receptores (bajo acoplamiento).
    Los fallos en un observador no bloquean a los demás.
    """

    def __init__(self) -> None:
        self._observers: dict[str, list[PipelineObserver]] = defaultdict(list)

    def subscribe(self, event_type: str, observer: PipelineObserver) -> None:
        self._observers[event_type].append(observer)
        logger.debug("[EventBus] %s suscrito a %s", observer.__class__.__name__, event_type)

    def unsubscribe(self, event_type: str, observer: PipelineObserver) -> None:
        self._observers[event_type] = [
            o for o in self._observers[event_type] if o is not observer
        ]

    def publish(self, event_type: str, payload: dict) -> None:
        payload.setdefault("event_type", event_type)
        payload.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
        for observer in self._observers.get(event_type, []):
            try:
                observer.update(event_type, payload)
            except Exception as exc:
                # Aislamos fallos por observador (buenas prácticas del patrón)
                logger.error("[EventBus] Fallo en observer %s: %s", observer.__class__.__name__, exc)


# ─────────────────────────────────────────────────────────────────
# Observadores concretos
# ─────────────────────────────────────────────────────────────────
class AuditLogObserver(PipelineObserver):
    """Registra en log estructurado cada evento del pipeline."""

    def update(self, event_type: str, payload: dict) -> None:
        logger.info(
            "[AUDIT] event=%s | ts=%s | details=%s",
            event_type, payload.get("timestamp"), payload
        )


@dataclass
class MetricsObserver(PipelineObserver):
    """Acumula métricas de ejecución del pipeline."""
    metrics: dict = field(default_factory=dict)

    def update(self, event_type: str, payload: dict) -> None:
        key = event_type
        self.metrics[key] = self.metrics.get(key, 0) + 1
        if event_type == PipelineEvent.RAW_FILE_LOADED:
            self.metrics["total_records_loaded"] = (
                self.metrics.get("total_records_loaded", 0) + payload.get("records", 0)
            )

    def summary(self) -> dict:
        return dict(self.metrics)


@dataclass
class PipelineTriggerObserver(PipelineObserver):
    """
    Al completarse el pipeline raw, guarda una señal en PostgreSQL
    para que el pipeline clean sepa que puede ejecutarse.
    """
    engine: Any

    def update(self, event_type: str, payload: dict) -> None:
        if event_type != PipelineEvent.RAW_PIPELINE_COMPLETED:
            return
        from sqlalchemy import text
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS raw._pipeline_events (
                        id SERIAL PRIMARY KEY,
                        event_type VARCHAR(100),
                        payload JSONB,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """))
                conn.execute(text("""
                    INSERT INTO raw._pipeline_events (event_type, payload)
                    VALUES (:et, :pl::jsonb)
                """), {"et": event_type, "pl": str(payload)})
            logger.info("[PipelineTriggerObserver] Evento %s registrado en BD", event_type)
        except Exception as exc:
            logger.error("[PipelineTriggerObserver] Error registrando evento: %s", exc)


# ─────────────────────────────────────────────────────────────────
# Bus singleton para usar en los bloques
# ─────────────────────────────────────────────────────────────────
event_bus = PipelineEventBus()
metrics_observer = MetricsObserver()
event_bus.subscribe(PipelineEvent.RAW_FILE_LOADED, AuditLogObserver())
event_bus.subscribe(PipelineEvent.RAW_FILE_LOADED, metrics_observer)
event_bus.subscribe(PipelineEvent.RAW_FILE_FAILED, AuditLogObserver())
event_bus.subscribe(PipelineEvent.RAW_PIPELINE_COMPLETED, AuditLogObserver())
event_bus.subscribe(PipelineEvent.CLEAN_PIPELINE_COMPLETED, AuditLogObserver())
