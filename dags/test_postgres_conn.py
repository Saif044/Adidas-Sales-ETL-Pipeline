from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def log_the_result(ti):
    # This pulls the result from the 'get_total_rows' task
    result = ti.xcom_pull(task_ids='get_total_rows')
    print(f"--- DATA CHALLENGE RESULT: The Adidas table has {result[0][0]} rows! ---")

with DAG(
    dag_id='adidas_sales_with_print',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    get_count = SQLExecuteQueryOperator(
        task_id='get_total_rows',
        conn_id='adidas_dataset_postgres',
        sql="SELECT COUNT(*) FROM public.adidas_sales;",
    )

    print_count = PythonOperator(
        task_id='print_to_logs',
        python_callable=log_the_result
    )

    get_count >> print_count