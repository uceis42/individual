# -*- coding: utf-8 -*-
"""
Created on Sun Apr 24 11:18:32 2022

@author: Shihao Zhou
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
import logging
import boto3
import botocore
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd
import io


log = logging.getLogger(__name__)

default_args = {
    'start_date': datetime(2022, 4, 14),
    'owner': 'Airflow',
    'filestore_base': '/tmp/airflowtemp/',
    'email_on_failure': False,
    'email_on_retry': False,
    'url_to_scrape': Variable.get("example_web_scraping_pipeline", deserialize_json=True)['url_to_scrape'],
    'aws_conn_id': "aws_default",
    'bucket_name': Variable.get("example_web_scraping_pipeline", deserialize_json=True)['bucket_name'],
    's3_key': Variable.get("example_web_scraping_pipeline", deserialize_json=True)['s3_key'],
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('DE166_web_scraping_pipeline',
          description='a web scraping pipeline that save the outout to a csv file in S3',
          schedule_interval='@weekly',
          catchup=False,
          default_args=default_args,
          max_active_runs=1)
def web_scraping_function(**kwargs):

    # Import packages


    # Specify url
    url = kwargs['url_to_scrape']
    base_url = urljoin(url, "/").rstrip("/")

    log.info('Going to scrape data from {0}'.format(url))

    # Package the request, send the request and catch the response: r
    headers = {"User-Agent": "Mozilla/5.0 (X11; CrOS x86_64 12871.102.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.141 Safari/537.36"}
    html = requests.get(url, headers=headers,timeout=10)

    # Extracts the response as html: html_doc


    # create a BeautifulSoup object from the HTML: soup
    soup = BeautifulSoup(html.text, 'lxml')

    # Find all 'h2' tags (which define hyperlinks): h_tags
    tableContent = soup.find_all("td", {"class": "govuk-table__cell"})

    companyContent = []
   
    log.info('Going to scrape data from website')

    # Iterate over all the h2 tags found to extract the link and 

    for i in range(51):
        
        companyContent.append(tableContent[i].find(text=True)) 
        log.info(i)
    

    
    return companyContent

def s3_save_file_func(**kwargs):

    

    bucket_name = kwargs['de166-data']
    key = kwargs['s3_key']
    s3 = S3Hook(kwargs['aws_conn_id'])

    # Get the task instance
    task_instance = kwargs['ti']

    # Get the output of the bash task
    scraped_data_previous_task = task_instance.xcom_pull(task_ids="web_scraping_task")

    log.info('xcom from web_scraping_task:{0}'.format(scraped_data_previous_task))

    # Load the list of dictionaries with the scraped data from the previous task into a pandas dataframe
    log.info('Loading scraped data into pandas dataframe')
    df = pd.DataFrame.from_dict(scraped_data_previous_task)

    log.info('Saving scraped data to {0}'.format(key))

    # Prepare the file to send to s3
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    # Save the pandas dataframe as a csv to s3
    s3 = s3.get_resource_type('s3')

    # Get the data type object from pandas dataframe, key and connection object to s3 bucket
    data = csv_buffer.getvalue()

    print("Saving CSV file")
    object = s3.Object(bucket_name, key)

    # Write the file to S3 bucket in specific path defined in key
    object.put(Body=data)

    log.info('Finished saving the scraped data to s3')

web_scraping_task = PythonOperator(
    task_id='web_scraping_task',
    provide_context=True,
    python_callable=web_scraping_function,
    op_kwargs=default_args,
    dag=dag,

)

save_scraped_data_to_s3_task = PythonOperator(
    task_id='save_scraped_data_to_s3_task',
    provide_context=True,
    python_callable=s3_save_file_func,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    op_kwargs=default_args,
    dag=dag,
)
web_scraping_task >> save_scraped_data_to_s3_task