# Example of using AzureBlobStorageBlobSensor in a DAG

from airflow import DAG
from datetime import datetime, timedelta

# Import the custom sensor created in the plugins directory
from azure_blob_storage_custom_sensor import AzureBlobStorageBlobSensor
from airflow.models import Variable

# Retrieve variables from the Airflow UI
container_name = Variable.get("container_name")
blob_name = Variable.get("blob_name")


# Define the DAG with a unique name and schedule
with DAG(
    "example_custom_sensor_dag",
    start_date=datetime(2023, 11, 8),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Task to wait for the specified blob to appear in the Azure Blob Storage container
    wait_for_blob = AzureBlobStorageBlobSensor(
        task_id="wait_for_blob_task",
        container_name=container_name,
        blob_name=blob_name,
        azure_conn_id='azureblob_cnn',
        poke_interval=30,  # Check every 30 seconds
        timeout=600  # Timeout after 10 minutes
    )

    # Define any subsequent tasks that depend on the blob being present
    wait_for_blob
