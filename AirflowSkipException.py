import pandas as pd
from datetime import datetime
import logging
import os

from airflow import DAG
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.utils.dates import days_ago


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


# Define the transformation function
def transform():
    try:
        ingest_data = pd.read_csv(local_dir+"/"+"sample-1.csv")
        if len(ingest_data) == 0:
            logging.info("No data to process; skipping transformation.")
            raise AirflowSkipException("Skipping as there is no data to transform.")
        else:
            ingest_data['city'] = "Chicago"
            ingest_data.to_parquet(local_dir+"/"+"sample.parquet", index=False)
            logging.info("Data transformation complete and saved as parquet.")
    except Exception as e:
        logging.error(f"Error during transformation: {e}")
        raise

with DAG(
    'sample_skip_if_empty_file',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 25),
    catchup=False  # Avoid running for past dates automatically
) as dag:

    # Task 1: Set up a sensor to detect a specific file in Azure Blob Storage
    ingest = WasbBlobSensor(
        task_id='wait_for_file',
        container_name=container_name,     # Using variable from Airflow UI
        blob_name=blob_name,               # Using variable from Airflow UI
        wasb_conn_id='azureblob_cnn',    # Connection ID for Azure Blob Storage
        timeout=600,                       # Time (in seconds) before the task times out
        poke_interval=30,                  # Time (in seconds) between each check
    )

    # Task 2: Download the file to local storage if it is found
    download_file = PythonOperator(
        task_id='download_file',
        python_callable=download_file_from_blob,
    )

    # Task 3: Transform the ingested data
    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    # Define end task
    end = DummyOperator(
        task_id="end"
    )

    # Set the pipeline flow
    ingest >> download_file>> transform_task >> end
