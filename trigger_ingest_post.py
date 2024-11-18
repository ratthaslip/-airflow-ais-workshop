
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Add TriggerDagRunOperator in etl_users DAG
trigger_ingest_post = TriggerDagRunOperator(
    task_id='trigger_ingest_user_post',
    trigger_dag_id='ingest_user_post',  # Target DAG ID
    wait_for_completion=False,  # Do not block execution
)
