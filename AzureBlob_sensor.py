from airflow import DAG
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import os

# Retrieve variables from the Airflow UI
container_name = Variable.get("container_name")
blob_name = Variable.get("blob_name")
local_dir = Variable.get("data_file_path")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Function to download the file from Azure Blob Storage
def download_file_from_blob():
    local_path = os.path.join(local_dir, os.path.basename(blob_name))
    
    # Ensure local directory exists
    os.makedirs(local_dir, exist_ok=True)
    
    # Download the file
    wasb_hook = WasbHook(wasb_conn_id='azureblob_cnn')
    wasb_hook.get_file(local_path, container_name, blob_name)
    
    print(f"File {blob_name} downloaded to {local_path}")

# Initialize the DAG
with DAG(
    'azure_blob_storage_sensor',
    default_args=default_args,
    description='A DAG that monitors Azure Blob Storage for file availability',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 1: Set up a sensor to detect a specific file in Azure Blob Storage
    wait_for_file = WasbBlobSensor(
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

    # Define task dependencies
    wait_for_file >> download_file
