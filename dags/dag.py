from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

from etl.extract import extract

with DAG(
    'etl',
    description='A simple etl pipeline to extract and load data to postgresql database',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily'
) as dag:
    
    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract
    )