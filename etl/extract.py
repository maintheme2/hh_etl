import requests
import pandas as pd
from fake_useragent import UserAgent
import os

from airflow.decorators import task

@task(task_id='extract_data')
def extract(**kwargs):
    job_titles = kwargs['job_titles']
    number_of_pages = kwargs['number_of_pages']
    """
    :param job_titles: list of job titles or keywords e.g. ['postgreSQL', 'data engineer'].
    :param number_of_pages: number of pages to be retrieved, reduce this if you want faster retrieval but be aware that for small number of pages some features will be missing.
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
        if not os.path.exists('../vacancies'):
            os.mkdir('../vacancies')
        df.to_csv('../vacancies/'+csv_name)