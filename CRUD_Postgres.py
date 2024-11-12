from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

# Define the schema name as a global variable
SCHEMA_NAME = "xxx"

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Initialize the DAG
with DAG(
    'crud_postgres',
    default_args=default_args,
    description='A sample DAG to interact with Azure PostgreSQL',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 1: Create schema if it doesn't exist within the specified database
    create_schema = PostgresOperator(
        task_id='create_schema',
        postgres_conn_id='azure_postgres_conn',  # Connection to the database
        sql=f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};",
    )

    # Task 2: Create a schema-qualified table if it doesn't exist
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='azure_postgres_conn',
        sql=f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.weekly_covid19_reports (
            id SERIAL PRIMARY KEY,
            year INTEGER,
            weeknum INTEGER,
            new_case INTEGER,
            total_case INTEGER,
            new_case_excludeabroad INTEGER,
            total_case_excludeabroad INTEGER,
            new_recovered INTEGER,
            total_recovered INTEGER,
            new_death INTEGER,
            total_death INTEGER,
            case_foreign INTEGER,
            case_prison INTEGER,
            case_walkin INTEGER,
            case_new_prev INTEGER,
            case_new_diff INTEGER,
            death_new_prev INTEGER,
            death_new_diff INTEGER,
            update_date TIMESTAMP
        );
        """,
    )

    # Task 3: Insert sample data into the schema-qualified table
    insert_data = PostgresOperator(
        task_id='insert_data',
        postgres_conn_id='azure_postgres_conn',
        sql=f"""
        INSERT INTO {SCHEMA_NAME}.weekly_covid19_reports (
            year, weeknum, new_case, total_case, new_case_excludeabroad, 
            total_case_excludeabroad, new_recovered, total_recovered, 
            new_death, total_death, case_foreign, case_prison, 
            case_walkin, case_new_prev, case_new_diff, death_new_prev, 
            death_new_diff, update_date
        ) VALUES (
            2024, 44, 569, 41711, 569, 41711, 
            0, 0, 0, 214, 0, 0, 569, 0, 569, 0, 0, NOW()
        );
        """,
    )

    # Task 4: Query data from the schema-qualified table and log the results
    query_data = PostgresOperator(
        task_id='query_data',
        postgres_conn_id='azure_postgres_conn',
        sql=f"SELECT * FROM {SCHEMA_NAME}.weekly_covid19_reports LIMIT 10;",
        do_xcom_push=True,  # Push results to XCom
    )

    # Set the order of execution
    create_schema >> create_table >> insert_data >> query_data

