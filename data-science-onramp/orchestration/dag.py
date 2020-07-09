"""
This DAG orchestrates all the components we have done so far, that is
* Data Ingestion
* Data Cleaning
* Feature Engineering
* Model Training
The DAG relies on four Airflow variables
(https://airflow.apache.org/concepts.html#variables)
* gcp_project - The Google Cloud Project for the Dataproc cluster
* gce_zone - The Dataproc cluster's region
* dataproc_cluster - The name of the dataproc_cluster
* gcs_bucket - The Google Cloud Storage bucket used throughout the pipeline
"""

import datetime

from airflow import models
from airflow.operators import bash_operator
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator

SESSION, VERSION = 9, 0

# Get Airflow varibles
PROJECT_ID = models.Variable.get('gcp_project')
BUCKET_NAME = models.Variable.get('gcs_bucket')
CLUSTER_NAME = models.Variable.get('dataproc_cluster')
REGION = models.Variable.get('gce_zone')

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry once after five minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    # Setting the start day to yesterday starts the DAG immediately when it is submitted
    'start_date': yesterday
}

with models.DAG(
        f'diego-tushar-v{SESSION}-{VERSION}',
        'catchup=False',
        default_args=default_args,
        schedule_interval=datetime.timedelta(days=1)) as dag:

    # Submit the setup job with the given arguments and other configuration
    # Note: This job is here for testing purposes, it will be removed later
    # The setup job is run once at the start and it the starting point of the pipeline
    setup_job = DataProcPySparkOperator(
        main=f'gs://{BUCKET_NAME}/setup.py',
        cluster_name=CLUSTER_NAME,
        arguments=[BUCKET_NAME, '--dry-run'],
        region=REGION,
        dataproc_pyspark_jars=['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
        task_id=f'setup-task-v{SESSION}-{VERSION}'
    )

    # Submit the clean job with the given arguments and other configuration
    clean_job = DataProcPySparkOperator(
        main=f'gs://{BUCKET_NAME}/clean.py',
        cluster_name=CLUSTER_NAME,
        arguments=[PROJECT_ID, BUCKET_NAME, '--dry-run'],
        region=REGION,
        dataproc_pyspark_jars=['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
        task_id=f'clean-task-v{SESSION}-{VERSION}'
    )

    # Declare a dependency between the setup job and the clean job
    # That is, the clean job can only run after the setup job is complete
    setup_job >> clean_job
