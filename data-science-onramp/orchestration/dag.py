import datetime

import airflow
from airflow.operators import bash_operator
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator

"""
SESSION = 4
with open('version.txt', mode='r') as f:
    session, version = int(f.readline()), int(f.readline())

VERSION = version + 1 if session == SESSION else 0

with open('version.txt', mode='w') as f:
    f.write(SESSION)
    f.write(VERSION)
"""

SESSION, VERSION = 4, 4

PROJECT_ID = 'data-science-onramp'
CLUSTER_NAME = 'data-cleaning'
REGION = 'us-east4'

start = datetime.datetime.now() - datetime.timedelta(minutes=10)

default_args = {
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(seconds=30),
    'start_date': start,
}

with airflow.DAG(
        f'diego-tushar-v{SESSION}-{VERSION}',
        'catchup=False',
        default_args=default_args,
        schedule_interval=datetime.timedelta(minutes=10)) as dag:

    setup_job = DataProcPySparkOperator(
        main='gs://citibikevd/diego-tushar-experience/setup.py',
        cluster_name='data-cleaning',
        arguments=['citibikevd', '--test'],
        region=REGION,
        dataproc_pyspark_jars=['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
        task_id=f'setup-task-v{SESSION}-{VERSION}'
    )

    clean_job = DataProcPySparkOperator(
        main='gs://citibikevd/diego-tushar-experience/clean.py',
        cluster_name='data-cleaning',
        arguments=['data-science-onramp', 'citibikevd', '--test'],
        region=REGION,
        dataproc_pyspark_jars=['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
        task_id=f'clean-task-v{SESSION}-{VERSION}'
    )

    setup_job >> clean_job
