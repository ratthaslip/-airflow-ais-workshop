from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Function to download a file from S3
def download_file_from_s3():
    s3_hook = S3Hook(aws_conn_id='aws_s3_conn')
    bucket_name = "airflow-ais-lab"
    key = "sample.csv"
    local_path = "/opt/airflow/dags/output/sample_data.txt"
    
    # Download file from S3
    s3_object = s3_hook.get_key(key, bucket_name)
    with open(local_path, 'wb') as f:
        s3_object.download_fileobj(f)
    
    print(f"Downloaded {key} from bucket {bucket_name} to {local_path}")

# Function to list and display files in the S3 bucket
def list_and_display_files_from_s3():
    s3_hook = S3Hook(aws_conn_id='aws_s3_conn')
    bucket_name = "airflow-ais-lab"
    files = s3_hook.list_keys(bucket_name=bucket_name)
    
    if files:
        print("Files in S3 bucket:")
        for file in files:
            print(file)
    else:
        print("No files found in S3 bucket.")

# Initialize the DAG
with DAG(
    'aws_s3_example',
    default_args=default_args,
    description='A sample DAG to interact with AWS S3',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 1: Download a file from S3
    download_file = PythonOperator(
        task_id='download_file',
        python_callable=download_file_from_s3,
    )

    # Task 2: List and display files in the S3 bucket
    list_files = PythonOperator(
        task_id='list_files',
        python_callable=list_and_display_files_from_s3,
    )

    # Define task dependencies
    list_files >> download_file 
