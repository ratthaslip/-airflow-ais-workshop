from airflow import DAG
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


with DAG(
    dag_id='example_short_circuit_2',
    start_date=datetime(2024, 1, 21),
    schedule="@daily",
    catchup=False
) as dag:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule="all_done")
    
    task1 = EmptyOperator(task_id='task1')
    task1_follow = EmptyOperator(task_id='task1_follow')
    
    task2 = EmptyOperator(task_id='task2')
    task2_follow = EmptyOperator(task_id='task2_follow')
    task2_print = PythonOperator(
        task_id="task2_print",
        python_callable=lambda: print("task2_print"),
        trigger_rule='always'
    )

    short_circuit_task1 = ShortCircuitOperator(
        task_id='short_circuit_task1',
        python_callable=lambda: True,
    )
    short_circuit_task2 = ShortCircuitOperator(
        task_id='short_circuit_task2',
        python_callable=lambda: False,
        ignore_downstream_trigger_rules=False
    )
    
    start >> short_circuit_task1 >> task1 >> task1_follow >> end
    start >> short_circuit_task2 >> task2 >> task2_follow >> task2_print >> end
    
