from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

def check_condition():
    # Condition logic here, return True or False
    return True  # or False

with DAG(
    dag_id='example_short_circuit', 
    start_date=datetime(2023, 10, 28), 
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    
    start = DummyOperator(task_id='start')
    
    short_circuit = ShortCircuitOperator(
        task_id='short_circuit',
        python_callable=check_condition
    )
    
    task_1 = DummyOperator(task_id='task_1')
    task_2 = DummyOperator(task_id='task_2')

    start >> short_circuit >> [task_1, task_2]
