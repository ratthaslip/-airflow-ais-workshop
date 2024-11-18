import pandas as pd

def merge_users_and_posts(**kwargs):
    # Paths for input files
    users_file = '/opt/airflow/dags/output/users.csv'
    posts_file = '/opt/airflow/dags/output/user_post.csv'
    merge_file = '/opt/airflow/dags/output/merge_users_posts.csv'

    # Read the users and posts data
    users_df = pd.read_csv(users_file)
    posts_df = pd.read_csv(posts_file)

    # Perform an inner join on the user ID
    merged_df = posts_df.merge(users_df, left_on='userId', right_on='ID', how='inner')

    # Save the merged data to a CSV file
    merged_df.to_csv(merge_file, index=False)
    print(f"Merged data saved to {merge_file}")


# Add merge task to ingest_user_post DAG
merge_task = PythonOperator(
    task_id='merge_users_and_posts',
    python_callable=merge_users_and_posts,
    provide_context=True,
    dag=dag,
)
