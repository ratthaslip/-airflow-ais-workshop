from airflow import DAG
from airflow.operators.python import PythonOperator
from azure.storage.blob import BlobServiceClient
from datetime import datetime

# Define constants
AZURE_ACCOUNT_NAME = "airflowlab2024"
AZURE_ACCESS_KEY = "" 
CONTAINER_NAME = 'ais-container'
BLOB_DIRECTORY = ""

# Initialize BlobServiceClient once, so it can be used in all functions
blob_service_client = BlobServiceClient(
    account_url=f"https://{AZURE_ACCOUNT_NAME}.blob.core.windows.net",
    credential=AZURE_ACCESS_KEY
)

# Define the function to list blobs
def list_blobs():
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    print("Blobs in the container:")
    for blob in container_client.list_blobs():
        print(blob.name)

# Define the function to download a specific blob
def download_blob(blob_name, download_path):
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
    with open(download_path, "wb") as download_file:
        download_data = blob_client.download_blob()
        download_data.readinto(download_file)
    print(f"Downloaded {blob_name} to {download_path}")

# Define the function to upload a file
def upload_blob(file_path, blob_name):
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    print(f"Uploaded {file_path} as {blob_name} to the container")

# Define the DAG
with DAG(
    dag_id="azure_blob_ingestion_with_upload",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Task to list blobs in the container
    list_blobs_task = PythonOperator(
        task_id="list_blobs",
        python_callable=list_blobs,
    )

    # Task to upload a file to the blob container
    upload_blob_task = PythonOperator(
        task_id="upload_blob",
        python_callable=upload_blob,
        op_args=["/opt/airflow/dags/test.txt",BLOB_DIRECTORY+"/"+"test_local.txt"]
    )

    # Task to download a specific blob
    download_blob_task = PythonOperator(
        task_id="download_blob",
        python_callable=download_blob,
        op_args=[BLOB_DIRECTORY+"/"+"test_local.txt","/opt/airflow/dags/output/test_blob.txt"]
    )

    # Task dependency setup
    #list_blobs_task >> download_blob_task >> upload_blob_task
    # Task dependency setup
    list_blobs_task  >> upload_blob_task >> download_blob_task
