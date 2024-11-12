from airflow import DAG
from airflow.providers.microsoft.azure.sensors.wasb import WasbPrefixSensor
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import os

# Retrieve variables from the Airflow UI
container_name = Variable.get("container_name")   # e.g., 'ais-container'
blob_prefix = Variable.get("blob_prefix")         # Prefix or directory inside container to monitor
local_dir = Variable.get("data_file_path")        # Local directory to store downloaded files

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Function to download files from the specified directory in Azure Blob Storage
def download_files_from_blob():
    # Initialize WasbHook and get the BlobServiceClient
    wasb_hook = WasbHook(wasb_conn_id='azureblob_cnn')
    blob_service_client = wasb_hook.get_conn()  # This provides the BlobServiceClient

    # Get the container client from BlobServiceClient
    container_client = blob_service_client.get_container_client(container_name)
    blobs = container_client.list_blobs(name_starts_with=blob_prefix)
    
    # Ensure local directory exists
    os.makedirs(local_dir, exist_ok=True)
    
    # Download each file in the directory
    for blob in blobs:
        blob_name = blob.name
        local_path = os.path.join(local_dir, os.path.basename(blob_name))
        wasb_hook.get_file(local_path, container_name, blob_name)
        print(f"File {blob_name} downloaded to {local_path}")

# Initialize the DAG
with DAG(
    'azure_blob_storage_prefix_sensor',
    default_args=default_args,
    description='A DAG that monitors Azure Blob Storage for any file in a specified directory',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 1: Set up a sensor to detect any file in the specified directory in Azure Blob Storage
    wait_for_files = WasbPrefixSensor(
        task_id='wait_for_files',
        container_name=container_name,     # Container name from Airflow variable
        prefix=blob_prefix,                # Monitor any files with the specified prefix
        wasb_conn_id='azureblob_cnn',      # Connection ID for Azure Blob Storage
        timeout=600,                       # Time (in seconds) before the task times out
        poke_interval=30,                  # Time (in seconds) between each check
    )

    # Task 2: Download all detected files to local storage
    download_files = PythonOperator(
        task_id='download_files',
        python_callable=download_files_from_blob,
    )

    # Define task dependencies
    wait_for_files >> download_files

