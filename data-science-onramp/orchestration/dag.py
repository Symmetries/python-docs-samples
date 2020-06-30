import datetime

import airflow
from airflow.operators import bash_operator
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator

PROJECT_ID = 'data-science-onramp'
CLUSTER_NAME = 'data-cleaning'
REGION = 'us-east4'
PYSPARK_JOB = {
        'reference': {'project_id': PROJECT_ID},
        'placement': {'cluster_name': CLUSTER_NAME},
        'pyspark_job': {'main_python_file_uri': 'gs://citibikevd/diego-tushar-experience/setup.py'}
}

start_date = datetime.datetime.now() - datetime.timedelta(minutes=1)

default_args = {
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': start_date,
}

with airflow.DAG(
        'diego-tushar-dummy-dag',
        'catchup=False',
        default_args=default_args,
        schedule_interval=datetime.timedelta(minutes=1)) as dag:

    # Submit a job to the cluster
    setup_job = DataProcPySparkOperator(
        main='gs://citibikevd/diego-tushar-experience/setup.py',
        cluster_name='data-cleaning',
        arguments=['citibikevd'],
        region=REGION,
        task_id='setup_task_4.0'
    )

    clean_job = DataProcPySparkOperator(
        main='gs://citibikevd/diego-tushar-experience/clean.py',
        cluster_name='data-cleaning_second_sequel',
        arguments=['citibikevd', 'data-science-onramp'],
        region=REGION,
        task_id='clean_task_4.0'
    )

    setup_job >> clean_job
