from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator  # For demonstration purposes
from custom_operators.file_read_operator import FileReadOperator  # Ensure this path is correct
#from file_read_operator import FileReadOperator  # Ensure this path is correct

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Initialize the DAG
with DAG(
    dag_id='file_read_dag',
    default_args=default_args,
    schedule_interval='@daily',  # Adjust schedule as needed
    catchup=False
) as dag:

    # Start task (Optional)
    start_task = DummyOperator(task_id='start')

    # Task using FileReadOperator
    file_read_task = FileReadOperator(
        task_id='read_file_task',
        file_path='/opt/airflow/dags/sample.csv'  # Replace with the actual file path
    )

    # Set task dependencies
    start_task >> file_read_task
