import random
from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.api.common.experimental.get_task_instance import get_task_instance


# function ในการสร้าง data 
def generate_data(**context):
    rand_int = random.randint(3, 9)
    return rand_int

# function ที่ใช้ในการดึงค่่า Xcom มาประมวลผลต่อ
def receive_data(**context):
    parent_dag_id = "parent_cross_dag_xcom_push_example"
    parent_task_id = "task3"
    child_data_interval_start = context["data_interval_start"]
    target_execution_date = child_data_interval_start - timedelta(hours=1)
    parent_task_instance = get_task_instance(
        dag_id=parent_dag_id,
        task_id=parent_task_id,
        execution_date=target_execution_date,
    )
    parent_task_instance_status = parent_task_instance.current_state()
    parent_xcom_value = parent_task_instance.xcom_pull(task_ids=parent_task_id)
    
    print(f"parent_task_instance: {parent_task_instance_status}")
    print(f"parent_xcom_from_task3 {parent_xcom_value}")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 10),
}

# สร้าง DAG/ Pipeline
with DAG(
    'parent_cross_dag_xcom_push_example',
    default_args=default_args,
    schedule_interval="0 2 * * *", # parent DAG ทำงานก่อน,
    catchup=True,
) as dag_parent:

    
    task1 = DummyOperator(task_id="task1")
    task2 = DummyOperator(task_id="task2")
    
    #Task ที่ทำหน้าที่ในการส่งค่า Xcom
    task3 = PythonOperator(
        task_id='task3',
        python_callable=generate_data,
        provide_context=True,  # (Default=True) อนุญาติให้ operator เข้าถึง context ซึ่งรวม Xcom ด้วย
    )
    
    task1 >> task2 >> task3
    
# สร้าง DAG/ Pipeline
with DAG(
    'child_cross_dag_xcom_pull_example',
    default_args=default_args,
    schedule_interval="0 3 * * *", # child DAG ทำงานทีหลัง
    catchup=True,
) as dag_child:

    
    #Task ที่ทำหน้าที่ในการรับค่า Xcom จาก Dag parent
    task1_child = PythonOperator(
        task_id='task1_child',
        python_callable=receive_data,
        provide_context=True,  # (Default=True) อนุญาติให้ operator เข้าถึง context ซึ่งรวม Xcom ด้วย
    )
        
    task2_child = DummyOperator(task_id="task2_child")
    task3_child = DummyOperator(task_id="task3_child")
    
    task1_child >> task2_child >> task3_child
    
    


