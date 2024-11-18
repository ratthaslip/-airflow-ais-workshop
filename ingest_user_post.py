from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import csv 

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'ingest_user_post',
    default_args=default_args,
    description='A simple data pipeline',
    schedule_interval=timedelta(days=1),
    catchup = False
)

def extract_data():
    response = requests.get('https://jsonplaceholder.typicode.com/posts')
    data = response.json()
    print(f"Data extracted: {data[:5]}")  # Print the first 5 records for validation
    return data

def transform_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='extract_data')
    filtered_data = [post for post in data if post['userId'] <= 5]
    print(f"Filtered data: {filtered_data[:5]}")  # Debugging print
    return filtered_data  # Explicitly return the filtered data

def save_to_csv(**kwargs):
    # Pull data from transform_data
    data = kwargs['ti'].xcom_pull(task_ids='transform_data')
    if not data:
        print("No data received from transform_data.")
        raise ValueError("No valid data to save to CSV.")

    # Print the received data for debugging
    print(f"Data received for saving: {data[:5]}")

    # Save data to CSV
    output_file = '/opt/airflow/dags/output/user_post.csv'
    fieldnames = data[0].keys()

    with open(output_file, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    print(f"Data successfully saved to {output_file}")


# Define tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

save_to_csv_task = PythonOperator(
    task_id='save_to_csv',
    python_callable=save_to_csv,
    provide_context=True,
    dag=dag,
)
# Set task dependencies
extract_task >> transform_task >> save_to_csv_task