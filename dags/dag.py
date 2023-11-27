from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from etl.extract import extract
from etl.load import load 

with DAG(
    'etl',
    description='A simple etl pipeline to extract and load data to postgresql database',
    start_date=datetime(2023, 11, 23),
    schedule_interval='@daily'
) as dag:
    
    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract,
        op_kwargs={
            'job_titles': ['postgreSQL', 'data engineer'],
            'number_of_pages': 50
        }
    )

    transform = BashOperator(
        task_id='transform_data',
        bash_command='jupyter nbconvert --execute --to notebook --inplace etl/transform.ipynb'
    )

    load = PythonOperator(
        task_id='load_data',
        python_callable=load,
        op_kwargs={
            'df': 'transformed.csv',
            'db_name': 'postgres',
            'db_user': 'temp_user',
            'db_password': 'temp_password'
        }
    )

    extract >> transform >> load