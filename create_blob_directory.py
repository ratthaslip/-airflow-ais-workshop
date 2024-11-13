import os
from azure.storage.blob import BlobServiceClient
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


# Accessing the environment variable
AZURE_ACCOUNT_NAME = "airflowlab2024"
AZURE_ACCESS_KEY = "" 

# Building connection string using the access key
AZURE_CONN_STRING = f"DefaultEndpointsProtocol=https;AccountName={AZURE_ACCOUNT_NAME};AccountKey={AZURE_ACCESS_KEY};EndpointSuffix=core.windows.net"
CONTAINER_NAME = "ais-container"
BLOB_DIRECTORY = ""

blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONN_STRING)


def create_directory_in_blob(container_name, folder_path):
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONN_STRING)
    container_client = blob_service_client.get_container_client(container_name)
    
    # Create a dummy blob to represent the directory
    blob_name = f"{folder_path}/"
    container_client.upload_blob(blob_name, b"")  # Empty content to simulate a directory

# DAG Definition
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 13),
}

with DAG(
     dag_id='create_directory_in_blob', 
     default_args=default_args, 
     schedule_interval=None
) as dag:
    
    create_directory_task = PythonOperator(
        task_id="create_directory",
        python_callable=create_directory_in_blob,
        op_kwargs={
            "container_name": CONTAINER_NAME,
            "folder_path": BLOB_DIRECTORY
        }
    )
    create_directory_task
