import subprocess
import os
from prefect import flow, task, get_run_logger

# ── Configuración ─────────────────────────────────────────────────────────────
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPT_CARGA = os.path.join(PROJECT_DIR, "cargar_motherduck.py")

# ── Tasks ─────────────────────────────────────────────────────────────────────

@task(name="Extracción y Carga — BCP a MotherDuck", retries=2, retry_delay_seconds=30)
def task_cargar_motherduck():
    logger = get_run_logger()
    logger.info("Iniciando carga de boletines BCP a MotherDuck...")
    result = subprocess.run(
        ["python3", SCRIPT_CARGA],
        capture_output=True, text=True, cwd=PROJECT_DIR
    )
    if result.returncode != 0:
        raise Exception(f"Error en carga:\n{result.stderr}")
    logger.info(result.stdout)
    logger.info("Carga completada exitosamente.")

@task(name="Transformación — dbt run", retries=1, retry_delay_seconds=15)
def task_dbt_run():
    logger = get_run_logger()
    logger.info("Ejecutando modelos dbt...")
    result = subprocess.run(
        ["dbt", "run", "--project-dir", PROJECT_DIR, "--profiles-dir", os.path.expanduser("~/.dbt")],
        capture_output=True, text=True, cwd=PROJECT_DIR
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Error en dbt run:\n{result.stderr}")
    logger.info("Modelos dbt ejecutados exitosamente.")

@task(name="Calidad de datos — dbt test", retries=1, retry_delay_seconds=15)
def task_dbt_test():
    logger = get_run_logger()
    logger.info("Ejecutando tests de calidad...")
    result = subprocess.run(
        ["dbt", "test", "--project-dir", PROJECT_DIR, "--profiles-dir", os.path.expanduser("~/.dbt")],
        capture_output=True, text=True, cwd=PROJECT_DIR
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Tests fallidos:\n{result.stderr}")
    logger.info("Todos los tests pasaron exitosamente.")

# ── Flow principal ─────────────────────────────────────────────────────────────

@flow(
    name="Pipeline BCP — Banco Central del Paraguay",
    description="Pipeline end-to-end: extracción boletines BCP → MotherDuck → dbt transformaciones → tests de calidad"
)
def pipeline_bcp():
    logger = get_run_logger()
    logger.info("=== Iniciando Pipeline BCP ===")

    # Paso 1 — EL
    task_cargar_motherduck()

    # Paso 2 — Transformación
    task_dbt_run()

    # Paso 3 — Calidad
    task_dbt_test()

    logger.info("=== Pipeline BCP completado exitosamente ===")

# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    pipeline_bcp()
