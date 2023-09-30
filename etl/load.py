import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy.engine import URL

df = pd.read_csv('transformed.csv')

db_name = input("Enter the name of the database to connect: ")
db_user = input("Enter your username for the database: ")
db_password = input("Enter your password for the database: ")

con = psycopg2.connect(
    host="localhost",
    database=db_name,
    user=db_user,
    password=db_password
)

cur = con.cursor()

url = URL.create("postgresql", username=db_user, password=db_password, host="localhost", database=db_name)

engine = create_engine(url)

df.to_sql('vacancies', con=engine, if_exists='replace', index=False)

if con:
    cur.close()
    con.close()