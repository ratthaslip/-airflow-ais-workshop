from airflow import DAG
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

# Function to check extended business logic (e.g., inventory, pricing, and sales period)
def retail_logic_check(**kwargs):
    # Simulate checking business conditions in a retail domain
    inventory_available = 100  # Example: Inventory level for the product
    minimum_required_inventory = 50  # Minimum inventory needed to proceed
    product_price = 20.0  # Current price of the product
    minimum_price = 15.0  # Minimum price allowed by business policy
    is_sales_period = True  # Check if the product is in an active sales period
    
    # Evaluate the business conditions
    inventory_check = inventory_available >= minimum_required_inventory
    price_check = product_price >= minimum_price
    sales_period_check = is_sales_period  # True if the sales period is active

    # Print checks for debugging purposes
    print(f"Inventory Check: {inventory_check} (Available: {inventory_available}, Required: {minimum_required_inventory})")
    print(f"Price Check: {price_check} (Product Price: {product_price}, Minimum Price: {minimum_price})")
    print(f"Sales Period Active: {sales_period_check}")

    # The overall business condition is met if all individual checks pass
    business_condition_met = inventory_check and price_check and sales_period_check
    
    # Store the results in XCom for downstream tasks
    kwargs['ti'].xcom_push(key='inventory_check', value=inventory_check)
    kwargs['ti'].xcom_push(key='price_check', value=price_check)
    kwargs['ti'].xcom_push(key='sales_period_check', value=sales_period_check)

    print(f"Business logic check result: {business_condition_met}")
    
    # Return True to proceed with downstream tasks, False to skip them
    return business_condition_met

# Task action that acts based on the results of business logic checks
def handle_business_logic_results(**kwargs):
    # Retrieve the check results from XCom
    inventory_check = kwargs['ti'].xcom_pull(key='inventory_check', task_ids='check_business_logic')
    price_check = kwargs['ti'].xcom_pull(key='price_check', task_ids='check_business_logic')
    sales_period_check = kwargs['ti'].xcom_pull(key='sales_period_check', task_ids='check_business_logic')
    
    # Handle different actions based on the business logic checks
    if inventory_check and price_check and sales_period_check:
        print("All business conditions met. Proceeding with business operations...")
        # Perform business operation like processing a sale
    else:
        if not inventory_check:
            print("Warning: Insufficient inventory. Triggering inventory alert!")
            # Add logic to trigger an inventory alert or reorder process
        if not price_check:
            print("Warning: Pricing rule violation. Logging pricing issue.")
            # Add logic to log or handle pricing rule violation
        if not sales_period_check:
            print("Warning: Sales period is inactive. Delaying promotions.")
            # Add logic to delay or stop promotion processing

# Define the DAG
with DAG(
    dag_id='retail_business_logic_check', 
    start_date=datetime(2023, 10, 28), 
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Start task
    start = DummyOperator(task_id='start')

    # Check the extended business logic before proceeding
    check_business_logic_task = ShortCircuitOperator(
        task_id='check_business_logic',
        python_callable=retail_logic_check,
        provide_context=True,  # Enables passing of the context
    )
    
    # Downstream task if business logic is satisfied
    handle_business_logic_task = PythonOperator(
        task_id='handle_business_logic_results',
        python_callable=handle_business_logic_results,
        provide_context=True,  # Enables passing of the context
    )

    # End task
    end = DummyOperator(task_id='end')

    # Set task dependencies for sequential execution
    start >> check_business_logic_task >> handle_business_logic_task >> end
