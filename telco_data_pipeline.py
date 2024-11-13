import pandas as pd
from datetime import datetime
import logging
import os

from airflow import DAG
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.dates import days_ago
from airflow.models import Variable


# Retrieve variables from the Airflow UI
container_name = Variable.get("container_name")
blob_name = Variable.get("blob_name")
local_dir = Variable.get("data_file_path")

# Function to download the file from Azure Blob Storage
def download_file_from_blob():
    local_path = os.path.join(local_dir, os.path.basename(blob_name))
    
    # Ensure local directory exists
    os.makedirs(local_dir, exist_ok=True)
    
    # Download the file
    wasb_hook = WasbHook(wasb_conn_id='azureblob_cnn')
    wasb_hook.get_file(local_path, container_name, blob_name)
    
    print(f"File {blob_name} downloaded to {local_path}")

# ShortCircuit to stop the workflow if the source file doesn't exist or is not valid
def check_source_data(file_path):
    try:
        df = pd.read_csv(file_path)
        return not df.empty
    except Exception:
        return False  # Short circuit if data is missing or invalid
    
# Function for the data quality check
def data_quality_check(file_path):
    df = pd.read_csv(file_path)
    
    # Example data quality check: Ensure there's data and all columns are not null
    if df.empty or df.isnull().any().any():
        return "skip_transform"  # Branch to skip load if quality fails
    return "transform_telco_data"  # Continue if quality is fine

# Function to transform the data
def transform_data(file_path):
    try:
        df = pd.read_csv(file_path)

        if len(df) == 0:
            logging.info("No data to process; skipping transformation.")
            raise AirflowSkipException("Skipping as there is no data to transform.")
        else:
            # Example transformation: Add a new column for record type
            df['record_type'] = 'transformed'
            df.to_parquet(local_dir+"/"+"transformed_telco_data.parquet")
            logging.info("Data transformation complete and saved as parquet.")

    except Exception as e:
        logging.error(f"Error during transformation: {e}")
        raise


with DAG(
    dag_id='telco_data_pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    
    # Step 1: Set up a sensor to detect a specific file in Azure Blob Storage
    wait_for_file = WasbBlobSensor(
        task_id='wait_for_file',
        container_name=container_name,     # Using variable from Airflow UI
        blob_name=blob_name,               # Using variable from Airflow UI
        wasb_conn_id='azureblob_cnn',    # Connection ID for Azure Blob Storage
        timeout=600,                       # Time (in seconds) before the task times out
        poke_interval=30,                  # Time (in seconds) between each check
    )

    # Step 2: Download the file to local storage if it is found
    download_file = PythonOperator(
        task_id='download_file',
        python_callable=download_file_from_blob,
    )

    # Step 3: Short circuit the workflow if the data doesn't exist or is empty
    short_circuit = ShortCircuitOperator(
        task_id="check_telco_data",
        python_callable=check_source_data,
        op_kwargs={"file_path": local_dir+"/"+"telco_dataset.csv"}
    )

    # Step 4: Data quality check using BranchPythonOperator
    quality_check = BranchPythonOperator(
        task_id='data_quality_check',
        python_callable=data_quality_check,
        op_kwargs={"file_path": local_dir+"/"+"telco_dataset.csv"},
        provide_context=True
    )

    # Step 5: Transform the data (if quality checks pass)
    transform = PythonOperator(
        task_id='transform_telco_data',
        python_callable=transform_data,
        op_kwargs={"file_path": local_dir+"/"+"telco_dataset.csv"}
    )

    # Step 6: Skip transform if the data quality check fails
    None_transform = DummyOperator(
        task_id="None_transform"
    )

    # Step 8: End task for branching
    end = DummyOperator(
        task_id="end",
        trigger_rule="all_done"
    )


    # DAG flow: Ingest -> ShortCircuit -> Quality Check -> Branch -> (Transform + Load) or Skip
    wait_for_file >> download_file>> short_circuit >> quality_check
    quality_check >> transform >> end
    quality_check >> None_transform >> end

