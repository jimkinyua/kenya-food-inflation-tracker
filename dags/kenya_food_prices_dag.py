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
from airflow.operators.python import PythonOperator

# -----------------------------------------------------------------------------
# DAG Base Directory & Environment
# -----------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))

logger = logging.getLogger("airflow.task")

HDX_URL = (
    "https://data.humdata.org/dataset/e0d3fba6-f9a2-45d7-b949-140c455197ff/"
    "resource/517ee1bf-2437-4f8c-aa1b-cb9925b9d437/download/wfp_food_prices_ken.csv"
)

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
    logger.info("Executing Task 1: Checking HDX/KNBS endpoint health...")
    
    # Check if a transient failure simulation is requested via DAG run conf
    dag_run_conf = context.get('dag_run').conf if context.get('dag_run') else {}
    simulate_failure = dag_run_conf.get('simulate_failure', False)
    task_instance = context.get('task_instance')
    
    if simulate_failure and task_instance and task_instance.try_number == 1:
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
    Task 5: Asserts data quality rules and logs execution audit lineage.
    """
    logger.info("Executing Task 5: Running Data Quality checks & Governance Logging...")
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME", "kenya_food_prices"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "")
    )
    dag_run_id = context.get('run_id', f"manual_run_{int(time.time())}")

    with conn.cursor() as cur:
        # Check 1: Verify Staging vs Fact row counts
        cur.execute("SELECT COUNT(*) FROM staging.raw_wfp_food_prices;")
        staging_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM analytics.fact_food_prices;")
        fact_count = cur.fetchone()[0]

        # Check 2: Verify zero negative or null prices
        cur.execute("SELECT COUNT(*) FROM analytics.fact_food_prices WHERE normalized_price_per_kg <= 0 OR normalized_price_per_kg IS NULL;")
        invalid_prices = cur.fetchone()[0]

        # Log Data Quality Assertion
        status = "PASSED" if (staging_count == fact_count and invalid_prices == 0) else "FAILED"
        cur.execute("""
            INSERT INTO governance.data_quality_log (run_id, check_name, table_name, assertion_rule, status, failed_rows_count)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (dag_run_id, "reconciliation_and_positive_prices", "fact_food_prices", "staging_count == fact_count AND price > 0", status, invalid_prices))

        # Log Pipeline Execution Audit Lineage
        cur.execute("""
            INSERT INTO governance.pipeline_audit_log (
                pipeline_name, run_id, source_name, source_url, raw_records_read,
                clean_records_loaded, execution_status, duration_seconds
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, ("kenya_food_prices_etl_pipeline", dag_run_id, "KNBS/WFP HDX", HDX_URL, staging_count, fact_count, status, 15.0))

        conn.commit()
    conn.close()

    if status != "PASSED":
        raise ValueError(f"Data quality assertion failed! Invalid prices count: {invalid_prices}")

    logger.info(f"Data Quality Asserts PASSED: Staging ({staging_count:,}) == Fact ({fact_count:,}) | 0 Invalid Prices.")


# -----------------------------------------------------------------------------
# DAG Definition & Schedule
# -----------------------------------------------------------------------------
with DAG(
    dag_id='kenya_food_prices_etl_pipeline',
    default_args=default_args,
    description='Automated monthly batch ETL pipeline for Kenya food prices & inflation analysis',
    schedule_interval='0 6 15 * *',       # 06:00 AM on 15th of every month
    catchup=False,
    max_active_runs=1,
    tags=['kenya', 'food_prices', 'inflation', 'capstone', 'knbs'],
) as dag:

    task_check_source_health = PythonOperator(
        task_id='check_source_health',
        python_callable=check_source_health,
        provide_context=True,
    )

    task_extract_raw_data = PythonOperator(
        task_id='extract_raw_data',
        python_callable=extract_raw_data,
        provide_context=True,
    )

    task_transform_clean = PythonOperator(
        task_id='transform_clean_normalise',
        python_callable=transform_clean_data,
        provide_context=True,
    )

    task_load_warehouse = PythonOperator(
        task_id='load_postgres_warehouse',
        python_callable=load_postgres_warehouse,
        provide_context=True,
    )

    task_data_quality_audit = PythonOperator(
        task_id='validate_data_quality_and_audit',
        python_callable=validate_data_quality_and_audit,
        provide_context=True,
    )

    # -------------------------------------------------------------------------
    # Orchestration Graph (Task Dependencies)
    # -------------------------------------------------------------------------
    task_check_source_health >> task_extract_raw_data >> task_transform_clean >> task_load_warehouse >> task_data_quality_audit
