"""
Script para crear los triggers/schedules de los pipelines en Mage.

Crea:
  1. raw_ingestion_daily    → ejecuta raw_ingestion cada día a las 02:00
  2. clean_after_raw        → ejecuta clean_transformation después de raw_ingestion
                             (tipo "event trigger" = pipeline completado)

Uso:
  # Desde el host (una vez que los contenedores estén corriendo):
  python scripts/create_triggers.py

  # O desde dentro del contenedor Mage:
  docker exec ny_taxi_mage python /home/src/scripts/create_triggers.py

Requisitos:
  pip install requests
"""
import os
import sys
import time
import json
import requests

MAGE_BASE_URL = os.environ.get("MAGE_BASE_URL", "http://localhost:6789")
API_HEADERS   = {"Content-Type": "application/json"}


def wait_for_mage(max_attempts: int = 30) -> None:
    """Espera a que Mage esté disponible antes de crear triggers."""
    print(f"Esperando que Mage esté disponible en {MAGE_BASE_URL}...")
    for attempt in range(max_attempts):
        try:
            r = requests.get(f"{MAGE_BASE_URL}/api/pipelines", timeout=5)
            if r.status_code < 500:
                print(f"Mage listo (intento {attempt + 1})")
                return
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(3)
    print("ERROR: Mage no respondió. Verifica que el contenedor esté corriendo.")
    sys.exit(1)


def get_existing_triggers(pipeline_uuid: str) -> list:
    url = f"{MAGE_BASE_URL}/api/pipelines/{pipeline_uuid}/pipeline_schedules"
    r   = requests.get(url, headers=API_HEADERS, timeout=10)
    if r.status_code == 200:
        return r.json().get("pipeline_schedules", [])
    return []


def create_trigger(pipeline_uuid: str, payload: dict) -> dict:
    existing = get_existing_triggers(pipeline_uuid)
    existing_names = [t.get("name") for t in existing]

    if payload["pipeline_schedule"]["name"] in existing_names:
        print(f"  [SKIP] Trigger '{payload['pipeline_schedule']['name']}' ya existe")
        return {}

    url = f"{MAGE_BASE_URL}/api/pipelines/{pipeline_uuid}/pipeline_schedules"
    r   = requests.post(url, json=payload, headers=API_HEADERS, timeout=10)
    if r.status_code in (200, 201):
        result = r.json().get("pipeline_schedule", {})
        print(f"  [OK] Trigger creado: {result.get('name')} (id={result.get('id')})")
        return result
    else:
        print(f"  [ERROR] HTTP {r.status_code}: {r.text}")
        return {}


def main():
    wait_for_mage()

    print("\n=== Creando trigger para raw_ingestion ===")
    # Trigger 1: Schedule diario para raw_ingestion (02:00 AM UTC)
    create_trigger("raw_ingestion", {
        "pipeline_schedule": {
            "name":              "raw_ingestion_daily_2am",
            "pipeline_uuid":     "raw_ingestion",
            "schedule_type":     "time",
            "start_time":        "2025-01-01T02:00:00",
            "schedule_interval": "@daily",
            "status":            "active",
            "description":       "Ejecuta ingesta raw de NY Taxi cada día a las 02:00 UTC",
        }
    })

    print("\n=== Creando trigger para clean_transformation ===")
    # Trigger 2: Schedule diario para clean (04:00 AM UTC, después del raw)
    create_trigger("clean_transformation", {
        "pipeline_schedule": {
            "name":              "clean_after_raw_4am",
            "pipeline_uuid":     "clean_transformation",
            "schedule_type":     "time",
            "start_time":        "2025-01-01T04:00:00",
            "schedule_interval": "@daily",
            "status":            "active",
            "description":       (
                "Ejecuta transformación clean cada día a las 04:00 UTC. "
                "Depende lógicamente de que raw_ingestion haya completado (02:00 AM)."
            ),
        }
    })

    print("\n=== Verificación ===")
    for pipeline in ["raw_ingestion", "clean_transformation"]:
        triggers = get_existing_triggers(pipeline)
        print(f"  {pipeline}: {len(triggers)} trigger(s) configurados")
        for t in triggers:
            print(f"    - {t.get('name')} | {t.get('status')} | {t.get('schedule_interval')}")

    print("\nTriggers configurados correctamente.")
    print("Accede a Mage UI en http://localhost:6789 para verificarlos visualmente.")


if __name__ == "__main__":
    main()
