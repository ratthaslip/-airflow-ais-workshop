import json
from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago

# Define the schema and table name as global variables
SCHEMA_NAME = "xxx"
TABLE_NAME = "weekly_covid19_reports"

# Function to save data into the PostgreSQL database
def save_data_into_db(**kwargs):
    # Pull data from the previous task's XCom
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='get_covid19_report_today')
    data = json.loads(data)
    
    # Establish PostgreSQL connection using PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='azure_postgres_conn')

    # Prepare the SQL INSERT statement
    insert_query = f"""
        INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} (
            year, weeknum, new_case, total_case, new_case_excludeabroad,
            total_case_excludeabroad, new_recovered, total_recovered,
            new_death, total_death, case_foreign, case_prison,
            case_walkin, case_new_prev, case_new_diff, death_new_prev,
            death_new_diff, update_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    # Insert each record into the database
    for record in data:
        # Map the fields from the API response to the table columns
        year = record["year"]
        weeknum = record["weeknum"]
        new_case = record["new_case"]
        total_case = record["total_case"]
        new_case_excludeabroad = record["new_case_excludeabroad"]
        total_case_excludeabroad = record["total_case_excludeabroad"]
        new_recovered = record["new_recovered"]
        total_recovered = record["total_recovered"]
        new_death = record["new_death"]
        total_death = record["total_death"]
        case_foreign = record["case_foreign"]
        case_prison = record["case_prison"]
        case_walkin = record["case_walkin"]
        case_new_prev = record["case_new_prev"]
        case_new_diff = record["case_new_diff"]
        death_new_prev = record["death_new_prev"]
        death_new_diff = record["death_new_diff"]
        update_date = record["update_date"]

        # Execute the insert query with parameters
        pg_hook.run(insert_query, parameters=(
            year, weeknum, new_case, total_case, new_case_excludeabroad,
            total_case_excludeabroad, new_recovered, total_recovered,
            new_death, total_death, case_foreign, case_prison,
            case_walkin, case_new_prev, case_new_diff, death_new_prev,
            death_new_diff, update_date
        ))

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 7, 1),
    'provide_context': True  # Supports task instance XCom
}

# Define the DAG
with DAG(
    'covid19_data_pipeline',
    schedule_interval='@daily',
    default_args=default_args,
    description='A data pipeline for COVID-19 report using PostgresHook',
    catchup=True
) as dag:

    # Task 1: Fetch data from the COVID-19 API
    t1 = SimpleHttpOperator(
        task_id='get_covid19_report_today',
        method='GET',
        http_conn_id='https_covid19_api',  # Replace with your HTTP connection ID
        endpoint='/api/Cases/today-cases-all',
        headers={"Content-Type": "application/json"},
        xcom_push=True,
    )

    # Task 2: Save the fetched data into PostgreSQL
    t2 = PythonOperator(
        task_id='save_data_into_db',
        python_callable=save_data_into_db,
    )

    # Set task dependencies
    t1 >> t2
