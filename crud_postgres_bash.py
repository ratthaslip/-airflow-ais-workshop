from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook

# Fetch connection details from Airflow
postgres_conn = BaseHook.get_connection("azure_postgres_conn")
host = postgres_conn.host
schema = postgres_conn.schema
login = postgres_conn.login
password = postgres_conn.password
port = postgres_conn.port

# Define the DAG
default_args = {
    'owner': 'airflow',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define SQL file path
sql_dir = "/opt/airflow/dags/sql"  # Update this path to the location of your SQL files

dag = DAG(
    'crud_postgres_bash',
    default_args=default_args,
    description='A sample DAG to interact with Azure PostgreSQL using BashOperator',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

# Define tasks using BashOperator
query_data = BashOperator(
    task_id='query_data',
    bash_command=f"PGPASSWORD={password} psql --host={host} --port={port} --username={login} --dbname={schema} --file={sql_dir}/query_data.sql",
    dag=dag,
)

query_data
