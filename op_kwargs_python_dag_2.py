from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Function definitions
def test_function(name, age):
    print(f"My name is {name}, and I am {age} years old")

def greet_function(greeting, name):
    print(f"{greeting}, {name}!")

def calculate_age(current_year, birth_year):
    age = current_year - birth_year
    print(f"You are {age} years old.")

# Define the DAG
my_dag = DAG(
    'op_kwargs_python_dag_2',
    start_date=datetime(2023, 2, 1),
    schedule_interval='@daily'
)

# Original task
python_task = PythonOperator(
    task_id='python_task',
    python_callable=test_function,
    op_kwargs={'name': 'Jack', 'age': 32},
    dag=my_dag
)

# New task 1: Greeting task
greeting_task = PythonOperator(
    task_id='greeting_task',
    python_callable= ,
    op_kwargs= ,
    dag=my_dag
)

# New task 2: Calculate age task
calculate_age_task = PythonOperator(
    task_id='calculate_age_task',
    python_callable=,
    op_kwargs= ,
    dag=my_dag
)

# Set task dependencies
python_task 
