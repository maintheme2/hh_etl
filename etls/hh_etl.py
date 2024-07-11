import requests
import pandas as pd
import nltk.tokenize 
import psycopg2
import os

from sqlalchemy import create_engine
from sqlalchemy.engine import URL


def extract(job_titles: list, number_of_pages: int):
    """
    :param job_titles: list of job titles or keywords e.g. ['postgreSQL', 'data engineer'].
    :param number_of_pages: number of pages to be retrieved, reduce this if you want faster retrieval 
                            but be aware that for small number of pages some features will be missing.
    """

    url = 'https://api.hh.ru/vacancies'
    # header = {
    #     'User-Agent': str(UserAgent().random)
    # }
    for job in job_titles:
        data=[]
        for i in range(number_of_pages):
            par = {'text': job, 'area':'113','per_page':'10', 'page':i}
            data.append(requests.get(url, params=par).json())
            vacancy_details = data[0]['items'][0].keys()
            df = pd.DataFrame(columns= list(vacancy_details))
            ind = 0
            for i in range(len(data)):
                for j in range(len(data[i]['items'])):
                    df.loc[ind] = data[i]['items'][j]
                    ind+=1
        csv_name = job+".csv"
        if not os.path.exists('./data'):
            os.mkdir('./data')
        df.to_csv('./data/input/'+csv_name)

def transform(path: str):

    df = pd.concat((pd.read_csv(path + '/input/' + f) for f in os.listdir(path + '/input/')), ignore_index=True)

    # Choose the type of the work that you want to see according to the following mapping:
    #    "{'id': 'probation', 'name': 'Стажировка'}",
    #    "{'id': 'full', 'name': 'Полная занятость'}",
    #    "{'id': 'part', 'name': 'Частичная занятость'}",
    #    "{'id': 'project', 'name': 'Проектная работа'}"], 

    for arr in df['employment']: 
        if '{' in arr:
            if (eval(arr)['id']) not in ['full', 'project']:  
                df.drop(df[df['employment'] == arr].index, inplace=True)
        else:
            if arr not in ['Полная занятость', 'Проектная работа']:
                df.drop(df[df['employment'] == arr].index, inplace=True)

    # Optimize this for your needs, i.e. provide some words that you 
    # don't want to see in the name of the vacancy
    for name in df['name']:
        for word in nltk.tokenize.wordpunct_tokenize(name):
            if word.lower() in ['java', 'php', 'qa', 'backend', 'back-end', 'back', 'frontend', 'front-end', 
                                'full-stack', 'devops', 'CTO', 'System Administrator', 'Администратор',
                                'fullstack' ,'c#', 'стажер-', 'стажировка', 'стажер', 'стажировка']:
                df.drop(df[df['name'] == name].index, inplace=True)

    df.dropna(axis=1, how='all', inplace=True)
    df.drop(columns=['Unnamed: 0', 'id', 'premium', 'department', 'archived', 'is_adv_vacancy', 'working_days', 
                    'working_time_intervals', 'working_time_modes', 'relations', 'apply_alternate_url', 
                    'accept_incomplete_resumes', 'response_letter_required', 'type', 'area', 
                    'published_at', 'url', 'show_logo_in_search'], inplace=True)
    df.fillna('null', inplace=True)


    for column in df.columns:
        if column == 'created_at':
            df[column] = pd.to_datetime(df[column])
            df[column] = df[column].dt.strftime('%Y-%m-%d %H:%M')
        if column in ['employer', 'experience', 'employment']:
            df[column] = df[column].apply(lambda x: eval(x)['name'] if x != 'null' else x)
        if column == 'salary':
            df = df.assign(salary_from=df.salary.apply(lambda x: eval(x)['from'] if x != 'null' else x))
            df = df.assign(salary_to=df.salary.apply(lambda x: eval(x)['to'] if x != 'null' else x))
            df.drop(columns=['salary'], inplace=True)
        if column == 'address':
            df[column] = df[column].apply(lambda x: eval(x)['city'] if x != 'null' else x)
        if column == 'professional_roles':
            df[column] = df[column].apply(lambda x: eval(x[1:-1])['name'] if x != 'null' else x)
        if column == 'snippet':
            df = df.assign(requirement=df.snippet.apply(lambda x: eval(x)['requirement'] if x != 'null' else x))
            df = df.assign(responsibility=df.snippet.apply(lambda x: eval(x)['responsibility'] if x != 'null' else x))
            df.drop(columns=['snippet'], inplace=True)
        
    print(df.head())
    df.to_csv(path + '/output/transformed.csv', index=False)


def load(**kwargs):
    df = pd.read_csv(kwargs['df'])
    db_name = kwargs['db_name']
    db_user = kwargs['db_user']
    db_password = kwargs['db_password']

    url = URL.create(drivername="postgresql", username="postgres", password="postgres", host="localhost", database="vacancies", port=5432)

    engine = create_engine(url)

    with engine.connect() as conn:
        df.to_sql(name='vacancies', con=conn, if_exists='replace', index=False)