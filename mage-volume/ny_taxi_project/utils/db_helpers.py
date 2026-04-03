"""
Helpers de conexión a PostgreSQL.
Las credenciales se leen desde variables de entorno (nunca hardcodeadas).
"""
from __future__ import annotations

import os
import logging
from functools import lru_cache
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


def get_connection_url() -> str:
    user     = os.environ["PG_USER"]
    password = os.environ["PG_PASSWORD"]
    host     = os.environ.get("PG_HOST", "postgres")
    port     = os.environ.get("PG_PORT", "5432")
    database = os.environ["PG_DATABASE"]
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"


@lru_cache(maxsize=1)
def get_engine():
    url = get_connection_url()
    engine = create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=10)
    logger.info("[db_helpers] Engine creado para %s", os.environ.get("PG_HOST", "postgres"))
    return engine
