"""
Production Apache Airflow DAG: Kenya Food Prices & Inflation Pipeline
======================================================================
Milestone 5 Orchestration:
- Schedule: Monthly on the 15th at 06:00 AM (0 6 15 * *) matching KNBS release cadence.
- Retries: 3 retries with 15s delay.
- Tasks:
    1. task_check_source_health   (HTTP / Endpoint connectivity check)
    2. task_extract_raw_data      (Atomic extraction to data/raw/ with MD5 verification)
    3. task_transform_clean       (Pandas normalization, unit conversion, inflation)
    4. task_load_warehouse        (Loads staging and Star Schema into PostgreSQL)
    5. task_data_quality_audit    (Asserts data health & logs lineage to governance schema)

Dependencies:
    check_source >> extract_raw >> transform_clean >> load_warehouse >> data_quality
"""

import os
import sys
import time
import subprocess
import logging
from datetime import datetime, timedelta
import urllib.request
import urllib.error
import psycopg2
from dotenv import load_dotenv

from airflow import DAG
try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator

# -----------------------------------------------------------------------------
# DAG Base Directory & Environment
# -----------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
env_file = os.path.join(BASE_DIR, ".env")
if not os.path.exists(env_file):
    # Fallback to standard installation location on VPS if run via symlink
    env_file = "/opt/kenya-food-market-analytics/.env"
load_dotenv(env_file)

logger = logging.getLogger("airflow.task")

HDX_URL = os.getenv("DATA_SOURCE_URL")
if not HDX_URL:
    raise ValueError(f"DATA_SOURCE_URL environment variable is missing from .env at {env_file}.")


# -----------------------------------------------------------------------------
# Default Arguments (Retries, Timeouts, Dependencies)
# -----------------------------------------------------------------------------
default_args = {
    'owner': 'james_kinyua',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=15),
    'execution_timeout': timedelta(minutes=15),
}

# -----------------------------------------------------------------------------
# Task Callables
# -----------------------------------------------------------------------------
def check_source_health(**context):
    """
    Task 1: Pings the HDX/KNBS data endpoint to verify server availability.
    Includes failure simulation support for rubric demonstration.
    """
    logger.info(f"Executing Task 1: Checking data endpoint health: {HDX_URL}")
    
    # Check if a transient failure simulation is requested via DAG run conf
    dag_run_conf = context.get('dag_run').conf if context.get('dag_run') else {}
    simulate_failure = dag_run_conf.get('simulate_failure', False)
    task_instance = context.get('task_instance')
    
    if simulate_failure and task_instance and getattr(task_instance, 'try_number', 1) == 1:
        logger.warning(">>> SIMULATING TRANSIENT NETWORK FAILURE (Attempt 1 of 3) <<<")
        raise ConnectionResetError("Simulated Connection Timeout: KNBS remote server failed to respond.")
    
    req = urllib.request.Request(HDX_URL, headers={"User-Agent": "AirflowHealthSensor/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        if resp.status != 200:
            raise ConnectionError(f"Source server returned non-200 status code: {resp.status}")
    logger.info("Health Check Passed: KNBS/HDX food prices endpoint is live and responding (HTTP 200).")


def extract_raw_data(**context):
    """Task 2: Executes batch extraction script."""
    logger.info("Executing Task 2: Extracting raw market price data...")
    python_bin = sys.executable
    script_path = os.path.join(BASE_DIR, "scripts", "extract_wfp_food_prices.py")
    res = subprocess.run([python_bin, script_path], capture_output=True, text=True, check=True)
    logger.info(res.stdout)


def transform_clean_data(**context):
    """Task 3: Executes pandas cleaning and normalisation pipeline."""
    logger.info("Executing Task 3: Normalising packaging units and calculating inflation...")
    python_bin = sys.executable
    script_path = os.path.join(BASE_DIR, "scripts", "transform_clean_food_prices.py")
    res = subprocess.run([python_bin, script_path], capture_output=True, text=True, check=True)
    logger.info(res.stdout)


def load_postgres_warehouse(**context):
    """Task 4: Loads staging table and Kimball Star Schema in PostgreSQL."""
    logger.info("Executing Task 4: Populating PostgreSQL Star Schema warehouse...")
    python_bin = sys.executable
    script_path = os.path.join(BASE_DIR, "scripts", "load_to_postgres.py")
    res = subprocess.run([python_bin, script_path], capture_output=True, text=True, check=True)
    logger.info(res.stdout)


def validate_data_quality_and_audit(**context):
    """
    Task 5: Asserts data quality rules and logs execution audit lineage via governance suite.
    """
    logger.info("Executing Task 5: Running Data Quality checks & Governance Logging...")
    python_bin = sys.executable
    script_path = os.path.join(BASE_DIR, "scripts", "run_data_quality_checks.py")
    res = subprocess.run([python_bin, script_path], capture_output=True, text=True, check=True)
    logger.info(res.stdout)
    if "100% PASSED Assertions" not in res.stdout:
        raise ValueError("Data quality audit did not achieve 100% passed assertions!")



# -----------------------------------------------------------------------------
# DAG Definition & Schedule
# -----------------------------------------------------------------------------
with DAG(
    dag_id='kenya_food_prices_etl_pipeline',
    default_args=default_args,
    description='Automated monthly batch ETL pipeline for Kenya food prices & inflation analysis',
    schedule='0 6 15 * *',       # 06:00 AM on 15th of every month
    catchup=False,
    max_active_runs=1,
    tags=['kenya', 'food_prices', 'inflation', 'capstone', 'knbs'],
) as dag:

    task_check_source_health = PythonOperator(
        task_id='check_source_health',
        python_callable=check_source_health,
    )

    task_extract_raw_data = PythonOperator(
        task_id='extract_raw_data',
        python_callable=extract_raw_data,
    )

    task_transform_clean = PythonOperator(
        task_id='transform_clean_normalise',
        python_callable=transform_clean_data,
    )

    task_load_warehouse = PythonOperator(
        task_id='load_postgres_warehouse',
        python_callable=load_postgres_warehouse,
    )

    task_data_quality_audit = PythonOperator(
        task_id='validate_data_quality_and_audit',
        python_callable=validate_data_quality_and_audit,
    )

    # -------------------------------------------------------------------------
    # Orchestration Graph (Task Dependencies)
    # -------------------------------------------------------------------------
    task_check_source_health >> task_extract_raw_data >> task_transform_clean >> task_load_warehouse >> task_data_quality_audit
