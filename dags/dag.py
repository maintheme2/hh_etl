import requests
import os
import sys
import pandas as pd
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etls.hh_etl import extract, transform, load

with DAG(
    'etl',
    description='A simple etl pipeline to extract and load data to postgresql database',
    start_date=datetime(2024, 7, 10),
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract,
        op_kwargs={
            'job_titles': ['etl-developer', 'data engineer'],
            'number_of_pages': 50
        }
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform,
        op_kwargs={
            'path': 'data'
        }
    )

    load = PythonOperator(
        task_id='load_data',
        python_callable=load,
        op_kwargs={
            'df': './data/output/transformed.csv',
            'db_name': 'vacancies',
            'db_user': 'vacancies',
            'db_password': 'vacancies'
        }
    )

    extract >> transform >> load