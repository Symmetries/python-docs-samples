"""
A DAG that orchestrates the entire end-to-end data science pipeline.

This DAG relies on four Airflow variables
https://airflow.apache.org/concepts.html#variables
* gcp_project - The Google Cloud Project that contains the pipeline components
* gce_zone - Google Compute Engine region of the Dataproc cluster
* dataproc_cluster - The name of the Dataproc Cluster
* gcs_bucket - The Google Cloud Storage bucket used to store intermediate results of the pipeline
"""

import datetime

from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.papermill_operator import PapermillOperator
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator


SESSION, VERSION = 11, 2

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
    # If a task fails, retry it once after waiting at least five minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    # Setting the start day to yesterday starts the DAG immediately when it is submitted
    'start_date': yesterday
}

sshCommand = "gcloud compute ssh feature-eng-high-ram \
        --zone=us-west1-b"
with models.DAG(
        f'diego-tushar-v{SESSION}-{VERSION}',
        'catchup=False',
        default_args=default_args,
        schedule_interval=datetime.timedelta(days=1)) as dag:

    # Submit the setup job with the given arguments and other configuration
    # Note: This job is here for testing purposes, it will be removed later
    setup_job = DataProcPySparkOperator(
        main=f'gs://{BUCKET_NAME}/setup.py',
        cluster_name=CLUSTER_NAME,
        arguments=[BUCKET_NAME, '--dry-run'],
        region=REGION,
        dataproc_pyspark_jars=['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
        task_id=f'setup-task-v{SESSION}-{VERSION}'
    )

    feature_eng_job = PapermillOperator(
            task_id='feature_engineering',
            input_nb='gs://feature_engineering.ipynb',
            output_nb='/dev/null',
            parameters="")

    # Define DAG dependencies
    setup_job >> clean_job
