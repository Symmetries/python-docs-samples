import datetime

from airflow import models
from airflow.operators import bash_operator
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator

SESSION, VERSION = 8, 0

PROJECT_ID = models.Variable.get('gcp_project')
BUCKET_NAME = models.Variable.get('gcs_bucket')
CLUSTER_NAME = models.Variable.get('dataproc_cluster')
REGION = models.Variable.get('gce_zone')

start = datetime.datetime.now() - datetime.timedelta(minutes=10)

default_args = {
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(seconds=30),
    'start_date': start
}

with models.DAG(
        f'diego-tushar-v{SESSION}-{VERSION}',
        'catchup=False',
        default_args=default_args,
        schedule_interval=datetime.timedelta(minutes=10)) as dag:

    setup_job = DataProcPySparkOperator(
        main=f'gs://{BUCKET_NAME}/setup.py',
        cluster_name=CLUSTER_NAME,
        arguments=[BUCKET_NAME, '--dry-run'],
        region=REGION,
        dataproc_pyspark_jars=['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
        task_id=f'setup-task-v{SESSION}-{VERSION}'
    )

    clean_job = DataProcPySparkOperator(
        main=f'gs://{BUCKET_NAME}/clean.py',
        cluster_name=CLUSTER_NAME,
        arguments=[PROJECT_ID, BUCKET_NAME, '--dry-run'],
        region=REGION,
        dataproc_pyspark_jars=['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
        task_id=f'clean-task-v{SESSION}-{VERSION}'
    )

    setup_job >> clean_job
