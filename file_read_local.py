from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Define the function to read the file content
def read_file(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
    print(f"File content:\n{content}")
    return content

# Initialize the DAG
with DAG(
    dag_id='file_read_local',
    default_args=default_args,
    schedule_interval='@daily',  # Adjust schedule as needed
    catchup=False
) as dag:

    # Start task (Optional)
    start_task = DummyOperator(task_id='start')

    # Task using PythonOperator to read the file
    file_read_task = PythonOperator(
        task_id='read_file_task',
        python_callable=read_file,
        op_args=['/opt/airflow/dags/sample.csv'],  # Replace with the actual file path
    )

    # Set task dependencies
    start_task >> file_read_task

