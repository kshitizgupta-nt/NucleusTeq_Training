from datetime import datetime
import logging
import random
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Set up the Airflow task logger
logger = logging.getLogger("airflow.task")

# Custom Cron Schedule for Pizza Shop: 
# Runs at minute 0 and 30, between 11 AM-1 PM (Lunch rush) and 5 PM-9 PM (Dinner rush) daily.
PIZZA_SHOP_SCHEDULE = "0,30 11-13,17-21 * * *"

default_args = {
    'owner': 'doughflow_pizza_co',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

def _receive_order(ti):
    order_id = f"PIZZA-{random.randint(1000, 9999)}"
    customer_status = random.choice(["VALID", "VALID", "VALID", "CANCELED"]) # 25% chance of cancellation
    
    # Push data to XCom to pass to subsequent tasks
    ti.xcom_push(key='order_id', value=order_id)
    ti.xcom_push(key='customer_status', value=customer_status)
    
    logger.info(f" [INFO] Received new order. Assigned ID: {order_id}")

def _check_cancellation(ti):
    order_id = ti.xcom_pull(task_ids='receive_order', key='order_id')
    status = ti.xcom_pull(task_ids='receive_order', key='customer_status')
    
    if status == "CANCELED":
        logger.warning(f" [CRITICAL] Order {order_id} was canceled by the customer!")
        return 'cancel_order'
    
    logger.info(f" [INFO] Order {order_id} is verified. Proceeding to kitchen.")
    return 'prep_dough_and_sauce'

def _cancel_order(ti):
    order_id = ti.xcom_pull(task_ids='receive_order', key='order_id')
    logger.warning(f" [WARN] Halting pipeline. Order {order_id} voided successfully.")

def _prep_dough_and_sauce(ti):
    order_id = ti.xcom_pull(task_ids='receive_order', key='order_id')
    logger.info(f" [INFO] Stretching dough and applying secret marinara sauce for {order_id}.")

def _quality_assurance(ti):
    order_id = ti.xcom_pull(task_ids='receive_order', key='order_id')
    logger.info(f" [INFO] Quality Check Passed: Melted cheese symmetry is optimal for {order_id}.")

with DAG(
    dag_id='pizza_delivery_pipeline',
    default_args=default_args,
    schedule=PIZZA_SHOP_SCHEDULE,
    catchup=False,
    tags=['kitchen', 'automation'],
) as dag:

    # 1. Receive Order (Python)
    receive_order = PythonOperator(
        task_id='receive_order',
        python_callable=_receive_order
    )

    # 2. Branching Logic (Branching Python)
    check_cancellation = BranchPythonOperator(
        task_id='check_cancellation',
        python_callable=_check_cancellation
    )

    # 3. Cancel Order - Skipped if order is valid (Python)
    cancel_order = PythonOperator(
        task_id='cancel_order',
        python_callable=_cancel_order
    )

    # 4. Prepare Dough (Python)
    prep_dough_and_sauce = PythonOperator(
        task_id='prep_dough_and_sauce',
        python_callable=_prep_dough_and_sauce
    )

    # 5. Bake Pizza (Bash Operator)
    bake_pizza = BashOperator(
        task_id='bake_pizza',
        bash_command='echo " [INFO] Blasting conveyor oven at 450┬░F..." && sleep 5'
    )

    # 6. Quality Check (Python)
    quality_assurance = PythonOperator(
        task_id='quality_assurance',
        python_callable=_quality_assurance
    )

    # 7. Dispatch Delivery (Bash Operator)
    dispatch_delivery = BashOperator(
        task_id='dispatch_delivery',
        bash_command='echo " [INFO] Driver dispatched. Hot box out the door!"'
    )

    # 8. Clean Join Operator (Fixes the branching skip bug)
    pipeline_end = EmptyOperator(
        task_id='pipeline_end',
        trigger_rule='none_failed_min_one_success'
    )

    # ---- DEPENDENCIES ----
    receive_order >> check_cancellation
    check_cancellation >> [cancel_order, prep_dough_and_sauce]
    
    # Path A: Success Route
    prep_dough_and_sauce >> bake_pizza >> quality_assurance >> dispatch_delivery >> pipeline_end
    
    # Path B: Cancellation Route
    cancel_order >> pipeline_end
