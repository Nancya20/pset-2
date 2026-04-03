"""
Script para ejecutar el pipeline raw_ingestion mes por mes.
Evita problemas de memoria procesando un mes a la vez.

Uso:
  python scripts/run_pipeline_by_month.py
  python scripts/run_pipeline_by_month.py --year 2024 --start-month 1 --end-month 12
"""
import os
import sys
import time
import argparse
import requests
import subprocess

MAGE_BASE_URL = os.environ.get("MAGE_BASE_URL", "http://localhost:6789")
ENV_FILE = os.path.join(os.path.dirname(__file__), "..", ".env")
PIPELINE_UUID = "raw_ingestion"
POLL_INTERVAL = 15   # segundos entre checks
MAX_WAIT = 1800      # 30 minutos máximo por mes


def read_env():
    env = {}
    with open(ENV_FILE) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env


def write_env_range(year, month):
    """Actualiza START_YEAR/MONTH y END_YEAR/MONTH en .env."""
    with open(ENV_FILE) as f:
        content = f.read()

    replacements = {
        "START_YEAR": str(year),
        "START_MONTH": str(month),
        "END_YEAR": str(year),
        "END_MONTH": str(month),
    }
    for key, val in replacements.items():
        import re
        content = re.sub(rf"^{key}=.*$", f"{key}={val}", content, flags=re.MULTILINE)

    with open(ENV_FILE, "w") as f:
        f.write(content)
    print(f"  .env actualizado: {year}-{month:02d}")


def restart_mage_and_copy():
    """Reinicia Mage para que tome las nuevas variables y copia archivos (sin mage-ai.db)."""
    print("  Reiniciando Mage...")
    subprocess.run(
        ["docker", "compose", "restart", "mage"],
        cwd=os.path.dirname(ENV_FILE),
        capture_output=True
    )
    time.sleep(15)

    print("  Copiando archivos al contenedor (preservando mage-ai.db)...")
    project_dir = os.path.join(os.path.dirname(ENV_FILE), "mage-volume", "ny_taxi_project")

    # Copia solo los directorios de código, no el estado interno de Mage
    dirs_to_copy = ["data_loaders", "transformers", "data_exporters", "utils", "pipelines"]
    for d in dirs_to_copy:
        src = os.path.join(project_dir, d, ".")
        dst = f"ny_taxi_mage://home/src/ny_taxi_project/{d}/"
        subprocess.run(["docker", "cp", src, dst], capture_output=True)

    # Copia archivos sueltos (no mage-ai.db)
    for fname in ["io_config.yaml", "requirements.txt", "__init__.py"]:
        src = os.path.join(project_dir, fname)
        if os.path.exists(src):
            subprocess.run(
                ["docker", "cp", src, "ny_taxi_mage://home/src/ny_taxi_project/"],
                capture_output=True
            )
    time.sleep(5)


def wait_for_mage():
    print("  Esperando Mage...", end="", flush=True)
    for _ in range(30):
        try:
            r = requests.get(f"{MAGE_BASE_URL}/api/pipelines", timeout=5)
            if r.status_code < 500:
                print(" listo")
                return True
        except Exception:
            pass
        print(".", end="", flush=True)
        time.sleep(3)
    print(" TIMEOUT")
    return False


def trigger_pipeline():
    """Lanza el pipeline y retorna el pipeline_run_id."""
    url = f"{MAGE_BASE_URL}/api/pipeline_schedules"
    payload = {
        "pipeline_schedule": {
            "name": f"monthly_run_{int(time.time())}",
            "pipeline_uuid": PIPELINE_UUID,
            "schedule_type": "time",
            "schedule_interval": "@once",
            "status": "active",
        }
    }
    r = requests.post(url, json=payload, timeout=10)
    if r.status_code not in (200, 201):
        print(f"  ERROR creando schedule: {r.status_code} {r.text[:200]}")
        return None

    schedule_id = r.json().get("pipeline_schedule", {}).get("id")
    if not schedule_id:
        return None

    # Trigger inmediato
    run_url = f"{MAGE_BASE_URL}/api/pipeline_schedules/{schedule_id}/pipeline_runs"
    r2 = requests.post(run_url, json={"pipeline_run": {}}, timeout=10)
    if r2.status_code not in (200, 201):
        print(f"  ERROR lanzando run: {r2.status_code} {r2.text[:200]}")
        return None

    run_id = r2.json().get("pipeline_run", {}).get("id")
    print(f"  Pipeline run iniciado (id={run_id}, schedule={schedule_id})")
    return run_id, schedule_id


def wait_for_completion(run_id):
    """Espera hasta que el pipeline_run complete o falle."""
    url = f"{MAGE_BASE_URL}/api/pipeline_runs/{run_id}"
    waited = 0
    while waited < MAX_WAIT:
        try:
            r = requests.get(url, timeout=10)
            status = r.json().get("pipeline_run", {}).get("status", "unknown")
            print(f"  Estado: {status} ({waited}s)", end="\r", flush=True)
            if status == "completed":
                print(f"\n  COMPLETADO en {waited}s")
                return True
            if status in ("failed", "cancelled"):
                print(f"\n  FALLIDO: {status}")
                return False
        except Exception as e:
            print(f"\n  Error consultando estado: {e}")
        time.sleep(POLL_INTERVAL)
        waited += POLL_INTERVAL
    print(f"\n  TIMEOUT después de {MAX_WAIT}s")
    return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--start-month", type=int, default=1)
    parser.add_argument("--end-month", type=int, default=12)
    args = parser.parse_args()

    print(f"\n=== Pipeline raw_ingestion: {args.year} meses {args.start_month}-{args.end_month} ===\n")

    results = []
    for month in range(args.start_month, args.end_month + 1):
        print(f"\n--- Procesando {args.year}-{month:02d} ---")

        write_env_range(args.year, month)
        restart_mage_and_copy()

        if not wait_for_mage():
            print("  Mage no disponible, saltando mes")
            results.append((month, "mage_unavailable"))
            continue

        result = trigger_pipeline()
        if not result:
            results.append((month, "trigger_failed"))
            continue

        run_id, schedule_id = result
        success = wait_for_completion(run_id)
        results.append((month, "ok" if success else "failed"))

    print("\n=== RESUMEN ===")
    for month, status in results:
        icon = "OK" if status == "ok" else "FAIL"
        print(f"  {icon} {args.year}-{month:02d}: {status}")


if __name__ == "__main__":
    main()
