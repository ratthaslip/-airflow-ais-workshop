from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

# Function to determine which branch to follow
def branch_test(**kwargs):
    # Check if the current date (ds_nodash) is even or odd
    if int(kwargs['ds_nodash']) % 2 == 0:
        return 'even_day_task'
    else:
        return 'odd_day_task'

# Define the DAG
with DAG(
    'branching_example',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Starting task
    start_task = DummyOperator(task_id="start_task")

    # Branching task
    branch_task = BranchPythonOperator(
        task_id='branch_task',
        provide_context=True,
        python_callable=branch_test
    )

    # Tasks for even day branch
    even_day_task = DummyOperator(task_id='even_day_task')
    even_day_task2 = DummyOperator(task_id='even_day_task2')

    # Tasks for odd day branch
    odd_day_task = DummyOperator(task_id='odd_day_task')
    odd_day_task2 = DummyOperator(task_id='odd_day_task2')

    # Define the branching flow
    start_task >> branch_task
    branch_task >> even_day_task >> even_day_task2
    branch_task >> odd_day_task >> odd_day_task2
