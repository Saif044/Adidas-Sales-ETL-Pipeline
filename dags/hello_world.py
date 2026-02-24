from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# 1. Define the Python function
def print_hello():
    print("Hello World! My first DAG is running.")

# 2. Define the DAG context
with DAG(
    dag_id='hello_world_dag',
    start_date=datetime(2023, 1, 1),
    schedule=None,  # CHANGED: 'schedule_interval' -> 'schedule'
    catchup=False
) as dag:

    # 3. Define the Task
    task1 = PythonOperator(
        task_id='say_hello',
        python_callable=print_hello
    )