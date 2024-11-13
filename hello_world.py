"""
This is a simple example DAG to demonstrate basic Airflow functionality.
"""

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator  # Updated import

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 4, 15),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG('hello_world', default_args=default_args)  # Updated DAG name to snake_case

# Define tasks
t1 = BashOperator(
    task_id='task_1',
    bash_command='echo "Hello World from Task 1"',
    dag=dag
)

t2 = BashOperator(
    task_id='task_2',
    bash_command='echo "Hello World from Task 2"',
    dag=dag
)

t3 = BashOperator(
    task_id='task_3',
    bash_command='echo "Hello World from Task 3"',
    dag=dag
)

t4 = BashOperator(
    task_id='task_4',
    bash_command='echo "Hello World from Task 4"',
    dag=dag
)

# Set dependencies
t2.set_upstream(t1)
t3.set_upstream(t1)
t4.set_upstream([t2, t3])
