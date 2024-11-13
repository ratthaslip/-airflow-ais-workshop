import pandas as pd
from airflow import DAG
from datetime import datetime
from airflow.providers.microsoft.azure.transfers.wasb_to_local import WasbToLocalOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.exceptions import AirflowSkipException

# Function for the data quality check
def data_quality_check(file_path):
    df = pd.read_csv(file_path)
    
    # Example data quality check: Ensure there's data and all columns are not null
    if df.empty or df.isnull().any().any():
        return "skip_load"  # Branch to skip load if quality fails
    return "load_data"  # Continue if quality is fine

# Function to transform the data
def transform_data(file_path):
    df = pd.read_csv(file_path)
    if df.empty:
        raise AirflowSkipException("No data to process")
    
    # Example transformation: Add a new column for record type
    df['record_type'] = 'transformed'
    df.to_parquet("/tmp/transformed_telco_data.parquet")

# ShortCircuit to stop the workflow if the source file doesn't exist or is not valid
def check_source_data(file_path):
    try:
        df = pd.read_csv(file_path)
        return not df.empty
    except Exception:
        return False  # Short circuit if data is missing or invalid

with DAG(
    dag_id='telco_data_reconciliation_pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    # Step 1: Ingest data from Azure Blob Storage
    ingest = WasbToLocalOperator(
        task_id="ingest_telco_data",
        container_name="my-container",
        blob_name="telco_dataset.csv",
        file_path="/tmp/telco_dataset.csv"
    )

    # Step 2: Short circuit the workflow if the data doesn't exist or is empty
    short_circuit = ShortCircuitOperator(
        task_id="check_telco_data",
        python_callable=check_source_data,
        op_kwargs={"file_path": "/tmp/telco_dataset.csv"}
    )

    # Step 3: Data quality check using BranchPythonOperator
    quality_check = BranchPythonOperator(
        task_id='data_quality_check',
        python_callable=data_quality_check,
        op_kwargs={"file_path": "/tmp/telco_dataset.csv"},
        provide_context=True
    )

    # Step 4: Transform the data (if quality checks pass)
    transform = PythonOperator(
        task_id='transform_telco_data',
        python_callable=transform_data,
        op_kwargs={"file_path": "/tmp/telco_dataset.csv"}
    )

    # Step 5: Load data into PostgreSQL
    load_data = PostgresOperator(
        task_id='load_telco_data',
        postgres_conn_id='your_postgres_connection',
        sql="""
            COPY my_table FROM '/tmp/transformed_telco_data.parquet' 
            WITH (FORMAT parquet);
        """
    )

    # Step 6: Skip loading if the data quality check fails
    skip_load = DummyOperator(
        task_id="skip_load"
    )

    # Step 7: End task for branching
    end = DummyOperator(
        task_id="end"
    )

    # DAG flow: Ingest -> ShortCircuit -> Quality Check -> Branch -> (Transform + Load) or Skip
    ingest >> short_circuit >> quality_check
    quality_check >> transform >> load_data >> end
    quality_check >> skip_load >> end
