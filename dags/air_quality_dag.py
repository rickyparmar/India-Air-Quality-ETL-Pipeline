#!/usr/bin/env python3
"""
Airflow DAG - India Air Quality ETL Pipeline
Orchestrates: extract_task >> transform_task >> load_task
Runs daily, with task-level retries and custom error logging callbacks.
"""

import os
import sys
import datetime
from datetime import timedelta

# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure the scripts directory is in python path to enable imports
sys.path.append("/opt/airflow")
from scripts.extract import extract_data
from scripts.transform import transform_data
from scripts.load import load_data


def log_failure_callback(context):
    """
    Custom on_failure_callback that logs task failures to standard output and writes
    them to a shared persistent file for external alerting/monitoring.
    """
    task_instance = context.get("task_instance")
    exception = context.get("exception")
    execution_date = context.get("execution_date")
    dag_id = context.get("dag_id")
    
    alert_message = (
        f"[{datetime.datetime.now().isoformat()}] ALERT: Task Failure Detected!\n"
        f"  DAG ID:         {dag_id}\n"
        f"  Task ID:        {task_instance.task_id if task_instance else 'N/A'}\n"
        f"  Execution Date: {execution_date}\n"
        f"  Error Details:  {exception}\n"
        f"----------------------------------------------------------------------\n"
    )
    
    # Print to console (will show up in task run logs)
    print(alert_message)
    
    # Write to a persistent shared logs file for operational auditing
    alert_log_file = "/opt/airflow/data/alerts.log"
    try:
        os.makedirs(os.path.dirname(alert_log_file), exist_ok=True)
        with open(alert_log_file, "a", encoding="utf-8") as f:
            f.write(alert_message)
    except Exception as e:
        print(f"Failed to write to local alerts file: {e}")


# Define wrapper functions to map ETL steps to PythonOperator executions
# Using XCom to pass file/folder paths rather than raw datasets is an Airflow best practice.
# It minimizes RAM usage on the Airflow database and handles large datasets scale-out safely.

def run_extract_task(**kwargs):
    """
    Executes extract.py and returns the raw file run directory.
    """
    # Fetch API_KEY from environment variables passed to the container
    api_key = os.getenv("API_KEY")
    if not api_key:
        raise ValueError("API_KEY environment variable is not defined in container context.")
        
    raw_dir_path = extract_data(api_key=api_key, output_base_dir="/opt/airflow/data/raw", limit=100)
    return raw_dir_path


def run_transform_task(**kwargs):
    """
    Retrieves the raw folder path from extract_task via XCom, transforms the data, and returns the cleaned CSV path.
    """
    # Pull directory path from the extraction task
    ti = kwargs["ti"]
    raw_data_dir = ti.xcom_pull(task_ids="extract_task")
    
    if not raw_data_dir:
        raise ValueError("Received null directory path from extract_task via XCom.")
        
    cleaned_csv_path = transform_data(raw_data_dir=raw_data_dir, processed_base_dir="/opt/airflow/data/processed")
    return cleaned_csv_path


def run_load_task(**kwargs):
    """
    Retrieves the cleaned CSV path from transform_task via XCom and upserts the data into PostgreSQL.
    """
    # Pull file path from the transformation task
    ti = kwargs["ti"]
    csv_file_path = ti.xcom_pull(task_ids="transform_task")
    
    if not csv_file_path:
        raise ValueError("Received null file path from transform_task via XCom.")
        
    load_data(csv_file_path=csv_file_path)


# Default arguments applied to all tasks
default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "start_date": datetime.datetime(2026, 7, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,                                # Fulfills task retry requirement
    "retry_delay": timedelta(minutes=5),         # Backoff delay between retries
    "on_failure_callback": log_failure_callback, # Task level failure callbacks
}

# Define the Airflow DAG
with DAG(
    dag_id="india_air_quality_etl",
    default_args=default_args,
    description="Daily ETL pipeline extracting real-time AQI readings from data.gov.in into PostgreSQL",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["analytics", "air_quality", "india"]
) as dag:

    # 1. Extraction task
    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=run_extract_task,
    )

    # 2. Transformation task
    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=run_transform_task,
    )

    # 3. Load task
    load_task = PythonOperator(
        task_id="load_task",
        python_callable=run_load_task,
    )

    # Define DAG task execution dependencies
    extract_task >> transform_task >> load_task
