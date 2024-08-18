
from airflow import DAG
from airflow.decorators import task, dag
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
import json
import logging

logger = logging.getLogger(__name__)

DATABASE_NAME = 'jobs'
BUCKET_NAME = 'jobs'

@dag(
    dag_id='find_jobs',
    default_args={
        'owner': 'me',
        'start_date': days_ago(1)
    },
    description='Runs the pipeline to extract jobs from LinkedIn once a day and load to PostgreSQL',
    schedule_interval='0 18 * * *',
    catchup=False
)
def find_jobs_pipeline():

    @task
    def init():
        logger.info('Initializing a table and a bucket')
        
        postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
        sql_query = """
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

            CREATE TABLE IF NOT EXISTS jobs (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                job_title VARCHAR(255),
                company_name VARCHAR(255),
                posted VARCHAR(255),
                job_link VARCHAR(255),
                location VARCHAR(255),
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        postgres_hook.run(sql_query)

        s3_hook = S3Hook(aws_conn_id='s3_conn')
        if not s3_hook.check_for_bucket(BUCKET_NAME):
            s3_hook.create_bucket(bucket_name=BUCKET_NAME)
            print(f'Bucket "{BUCKET_NAME}" created successfully.')
        else:
            print(f'Bucket "{BUCKET_NAME}" already exists.')

    @task
    def map_search_tasks():
        import itertools

        locations = Variable.get('locations').split(', ')
        keywords_list = Variable.get('keywords_list').split(', ')
        logger.info(f'Map: locations = {locations}, keywords = {keywords_list}')
        return [{'location': lk[0], 'keywords': lk[1]} for lk in itertools.product(locations, keywords_list)]

    @task
    def extract_jobs(search_parameters):
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.chrome.options import Options
        from linkedin_job_scraper.job_search import JobSearch

        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')  # Bypass OS security model (for Docker)
        chrome_options.add_argument('--disable-dev-shm-usage')  # Overcome limited resource problems (for Docker)
        
        driver = webdriver.Chrome(service=Service(), options=chrome_options)

        logger.info(f'Extracting jobs for {search_parameters}')
        search = JobSearch(driver, **search_parameters)
        try:
            jobs = search.get_jobs()
            logger.info(f'Found {len(jobs)}')

        except Exception as e:
            logger.error(f'Error occurred while extracting jobs: {e}')
            return
        
        filename = '_'.join(search_parameters.values()) + '.json'
        json_data = json.dumps(jobs)

        s3_hook = S3Hook(aws_conn_id='s3_conn')
        s3_hook.load_string(
            bucket_name=BUCKET_NAME,
            key=filename,
            string_data=json_data,
            replace=True
        )
        return filename

    @task
    def load_jobs(objects_names):
        import pandas as pd
        jobs_df = pd.DataFrame()

        for name in objects_names:
            s3_hook = S3Hook(aws_conn_id='s3_conn')
            data = s3_hook.read_key(bucket_name=BUCKET_NAME, key=name)
            jobs_df = pd.concat([jobs_df, pd.DataFrame(json.loads(data))], ignore_index=True)
        jobs_df = jobs_df.drop_duplicates(subset=['job_title', 'company_name']) # the same job can be found by different keywords

        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
        rows = [tuple(x) for x in jobs_df.to_numpy()]

        for i, row in enumerate(rows):
            try:
                pg_hook.insert_rows(
                    table='jobs',
                    rows=[row],
                    target_fields=jobs_df.columns.tolist()
                )
            except Exception as e:
                logger.error(f'Error occurred while loading job {i+1}: {e}')
                continue
        
        logger.info(f'Loaded {i} jobs to the database')
    
    init_task = init()
    map_task = map_search_tasks()
    extracted_objects = extract_jobs.partial().expand(search_parameters=map_task)
    load_jobs(extracted_objects)

    init_task >> map_task # to set the correct task order

find_jobs_pipeline()
