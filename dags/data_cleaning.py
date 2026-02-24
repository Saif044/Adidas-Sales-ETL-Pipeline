from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

# --- Transformation Logic ---
def transform_data(ti):
    # 1. Pull data from the 'extract' task via XCom
    raw_data = ti.xcom_pull(task_ids='extract_raw_data')
    
    # 2. Convert to DataFrame for easy cleaning
    df = pd.DataFrame(raw_data, columns=[
        'product', 'price_per_unit', 'units_sold', 'total_sales', 'operating_profit'
    ])

    # 3. CLEANING: Remove '$', '%', and ',' then convert to numeric
    # This addresses the formatting issues seen in the Excel source
    for col in ['price_per_unit', 'total_sales', 'operating_profit']:
        df[col] = df[col].replace(r'[\$,]', '', regex=True).astype(float)
    
    df['units_sold'] = df['units_sold'].replace(r'[,]', '', regex=True).astype(int)

    # 4. CALCULATION: Group by product and calculate Profit per Unit
    summary = df.groupby('product').agg({
        'units_sold': 'sum',
        'operating_profit': 'sum'
    }).reset_index()
    
    summary['profit_per_unit'] = summary['operating_profit'] / summary['units_sold']

    # 5. Return as a list of tuples for Postgres insertion
    return summary.values.tolist()

def load_data_to_postgres(ti):
    # Pull the transformed data
    data_to_load = ti.xcom_pull(task_ids='transform_and_clean')
    
    # Create the Hook using your connection ID
    hook = PostgresHook(postgres_conn_id='adidas_dataset_postgres')
    
    # Define the target table and columns
    target_fields = ['product', 'total_units_sold', 'total_profit', 'profit_per_unit']
    
    # The 'insert_rows' method is built for this! 
    # It handles the SQL and looping for you.
    hook.insert_rows(
        table='public.adidas_sales_summary',
        rows=data_to_load,
        target_fields=target_fields
    )

# 2. Update your DAG definition
with DAG(
    dag_id='adidas_transformation_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    extract = SQLExecuteQueryOperator(
        task_id='extract_raw_data',
        conn_id='adidas_dataset_postgres',
        sql="SELECT product, price_per_unit, units_sold, total_sales, operating_profit FROM public.adidas_sales;"
    )

    transform = PythonOperator(
        task_id='transform_and_clean',
        python_callable=transform_data
    )

    # 3. New Load Task using the function above
    load = PythonOperator(
        task_id='load_to_summary_table',
        python_callable=load_data_to_postgres
    )

    extract >> transform >> load