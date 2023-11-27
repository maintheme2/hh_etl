import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy.engine import URL

from airflow.decorators import task

@task(task_id='load_data')
def load(**kwargs):
    df = kwargs['df']
    db_name = kwargs['db_name']
    db_user = kwargs['db_user']
    db_password = kwargs['db_password']

    url = URL.create(drivername="postgresql", username=db_user, password=db_password, host="localhost", database=db_name)

    engine = create_engine(url)

    df.to_sql('vacancies', con=engine, if_exists='replace', index=False)