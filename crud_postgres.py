from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

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

    # Task 1: Create a table if it doesn't exist
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='azure_postgres_conn',
        sql="""
        CREATE TABLE IF NOT EXISTS daily_covid19_reports (
            id SERIAL PRIMARY KEY,
            txn_date TIMESTAMP,
            new_case INTEGER,
            total_case INTEGER,
            total_case_excludeabroad INTEGER,
            new_death INTEGER,
            total_death INTEGER,
            new_recovered INTEGER,
            total_recovered INTEGER,
            update_date TIMESTAMP
        );
        """,
    )

    # Task 2: Insert sample data
    insert_data = PostgresOperator(
        task_id='insert_data',
        postgres_conn_id='azure_postgres_conn',
        sql="""
        INSERT INTO daily_covid19_reports (txn_date, new_case, total_case, total_case_excludeabroad, new_death, total_death, new_recovered, total_recovered, update_date)
        VALUES (NOW(), 100, 1000, 900, 5, 50, 70, 800, NOW());
        """,
    )

    # Task 3: Query data and log the results
    query_data = PostgresOperator(
        task_id='query_data',
        postgres_conn_id='azure_postgres_conn',
        sql="SELECT * FROM daily_covid19_reports LIMIT 10;",
        do_xcom_push=True,  # Push results to XCom
    )
    

    # Set the order of execution
    create_table >> insert_data >> query_data
