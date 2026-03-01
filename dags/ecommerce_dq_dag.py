from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import os
import shutil
import requests
import sys

# Configure Python path for module importation
sys.path.append('/opt/airflow/')
from scripts.generator import generate_sales_data
from scripts.validator import run_quality_firewall

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2026, 2, 25),
    'retries': 0
}

def check_quality_and_branch(**kwargs):
    """
    Executes the data quality firewall against the raw data file.
    Returns the task_id of the next downstream task based on validation results.
    """
    input_file_path = "/opt/airflow/data/raw/sales_data.csv"
    is_clean = run_quality_firewall(input_path=input_file_path)
    
    if is_clean:
        return 'load_to_data_warehouse'
    else:
        return 'move_to_quarantine'

def quarantine_and_alert(**kwargs):
    """
    Isolates the invalid data file into the quarantine directory and 
    triggers a structured webhook alert to Discord.
    """
    raw_path = "/opt/airflow/data/raw/sales_data.csv"
    quarantine_dir = "/opt/airflow/data/quarantine/"
    
    os.makedirs(quarantine_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_reference = f"bad_sales_data_{timestamp}.csv"
    quarantine_path = os.path.join(quarantine_dir, file_reference)
    
    # Check for file existence to handle task idempotency
    if os.path.exists(raw_path):
        shutil.move(raw_path, quarantine_path)
        print(f"Data isolated successfully: {quarantine_path}")
    else:
        print(f"Source file not found or already moved: {raw_path}")
    
    # Dispatch a structured Discord alert using the Embed format
    discord_url = os.getenv("DISCORD_WEBHOOK_URL")
    if discord_url:
        payload = {
            "username": "Data Quality Monitor",
            "embeds": [
                {
                    "title": "Data Quality Exception Report",
                    "description": "Validation failed for E-commerce sales data. The file has been isolated to prevent pipeline contamination.",
                    "color": 16711680, # Hex color code for Red
                    "fields": [
                        {"name": "Pipeline", "value": "ecommerce_data_quality_firewall", "inline": True},
                        {"name": "Action Taken", "value": "File moved to quarantine", "inline": True},
                        {"name": "File Reference", "value": file_reference, "inline": False}
                    ],
                    "footer": {
                        "text": f"Alert generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    }
                }
            ]
        }
        try:
            response = requests.post(discord_url, json=payload)
            response.raise_for_status()
            print("Alert dispatched successfully.")
        except requests.exceptions.RequestException as e:
            print(f"Failed to dispatch alert: {e}")

# Define the Directed Acyclic Graph
with DAG(
    'ecommerce_data_quality_firewall',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Automated data pipeline for E-commerce sales data validation'
) as dag:

    start_pipeline = EmptyOperator(task_id='start_pipeline')

    generate_data = PythonOperator(
        task_id='generate_mock_sales_data',
        python_callable=generate_sales_data,
        op_kwargs={'output_path': '/opt/airflow/data/raw/sales_data.csv'}
    )

    quality_gate = BranchPythonOperator(
        task_id='data_quality_gate',
        python_callable=check_quality_and_branch
    )

    load_to_dw = EmptyOperator(task_id='load_to_data_warehouse')

    quarantine_data = PythonOperator(
        task_id='move_to_quarantine',
        python_callable=quarantine_and_alert
    )

    end_pipeline = EmptyOperator(
        task_id='end_pipeline',
        trigger_rule='none_failed_or_skipped'
    )

    # Define task dependencies
    start_pipeline >> generate_data >> quality_gate
    quality_gate >> load_to_dw >> end_pipeline
    quality_gate >> quarantine_data >> end_pipeline