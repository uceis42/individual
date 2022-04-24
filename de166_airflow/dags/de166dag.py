from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook

from datetime import datetime
from datetime import timedelta
import logging


log = logging.getLogger(__name__)


# =============================================================================
# 1. Set up the main configurations of the dag
# =============================================================================

default_args = {
    'start_date': datetime(2022, 4, 14),
    'owner': 'Airflow',
    'filestore_base': '/tmp/airflowtemp/',
    'email_on_failure': False,
    'email_on_retry': False,
    'url_to_scrape': Variable.get("web_scraping_pipeline", deserialize_json=True)['url_to_scrape'],
    'aws_conn_id': "S3",
    'bucket_name': Variable.get("web_scraping_pipeline", deserialize_json=True)['bucket_name'],
    's3_key': Variable.get("web_scraping_pipeline", deserialize_json=True)['s3_key'],
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('web_scraping_pipeline',
          description='Example of a web scraping pipeline that save the outout to a csv file in S3',
          schedule_interval='@weekly',
          catchup=False,
          default_args=default_args,
          max_active_runs=1)

# =============================================================================
# 2. Define different functions
# =============================================================================

def web_scraping_function(**kwargs):

    # Import packages
    import requests
    from bs4 import BeautifulSoup
    from urllib.parse import urljoin

    # Specify url
    url = kwargs['url_to_scrape']
    base_url = urljoin(url, "/").rstrip("/")

    log.info('Going to scrape data from {0}'.format(url))

    # Package the request, send the request and catch the response: r
    r = requests.get(url)

    # Extracts the response as html: html_doc
    html_doc = r.text

    # create a BeautifulSoup object from the HTML: soup
    soup = BeautifulSoup(html_doc)

    # Find all 'h2' tags (which define hyperlinks): h_tags
    h2_tags = soup.find_all("td", {"class": "govuk-table__cell"})

    scraped_data = []
    log.info('Going to scrape data from website')

    # Iterate over all the h2 tags found to extract the link and title of the blog posts
    for h2 in h2_tags:
        try:
            scraped_data.append(h2.text)
        except AttributeError as e:
            # Adding the handling of errors
            print('Tag was not found')
            continue

    log.info('Finished scraping the data')

    return scraped_data

def s3_save_file_func(**kwargs):

    import pandas as pd
    import io

    bucket_name = kwargs['bucket_name']
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


# =============================================================================
# 3. Set up the main configurations of the dag
# =============================================================================

web_scraping = PythonOperator(
    task_id='web_scraping',
    provide_context=True,
    python_callable=web_scraping_function,
    op_kwargs=default_args,
    dag=dag,

)

save_scraped_data_to_s3 = PythonOperator(
    task_id='save_scraped_data_to_s3',
    provide_context=True,
    python_callable=s3_save_file_func,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    op_kwargs=default_args,
    dag=dag,

)

# =============================================================================
# 4. Indicating the order of the dags
# =============================================================================

web_scraping >> save_scraped_data_to_s3