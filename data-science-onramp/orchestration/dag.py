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
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.papermill_operator import PapermillOperator
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
from airflow.contrib.operators.gcp_container_operator import GKEClusterCreateOperator, GKEClusterDeleteOperator, GKEPodOperator
from airflow.contrib.operators.mlengine_operator import MLEngineTrainingOperator, MLEngineVersionOperator
from google.cloud.container_v1.types import Cluster, NodePool, NodeConfig

import pandas as pd
import uuid

SESSION, VERSION = 21, 2

# Get Airflow varibles
PROJECT_ID = models.Variable.get('gcp_project')
BUCKET_NAME = models.Variable.get('gcs_bucket')
REGION = models.Variable.get('gce_region')
ZONE = models.Variable.get('gce_zone')
DATAPROC_CLUSTER_NAME = models.Variable.get('dataproc_cluster')
GKE_CLUSTER_NAME = f'{uuid.uuid4()}'

node_config = NodeConfig(machine_type='n1-standard-16')
node_pool = NodePool(initial_node_count=1, config=node_config)
GKE_CLUSTER = Cluster(name='tiego', initial_node_count=1, node_config=node_config)

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
    # The setup job is run once at the start and it the starting point of the pipeline
    # setup_job = DataProcPySparkOperator(
    #     main=f'gs://{BUCKET_NAME}/setup.py',
    #     cluster_name=DATAPROC_CLUSTER_NAME,
    #     arguments=[BUCKET_NAME, '--dry-run'],
    #     region=REGION,
    #     dataproc_pyspark_jars=['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
    #     task_id=f'setup-task-v{SESSION}-{VERSION}'
    # )

    # # Submit the clean job with the given arguments and other configuration
    # clean_job = DataProcPySparkOperator(
    #     main=f'gs://{BUCKET_NAME}/clean.py',
    #     cluster_name=DATAPROC_CLUSTER_NAME,
    #     arguments=[PROJECT_ID, BUCKET_NAME, '--dry-run'],
    #     region=REGION,
    #     dataproc_pyspark_jars=['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
    #     task_id=f'clean-task-v{SESSION}-{VERSION}'
    # )

    # feature_eng_job = PythonOperator(
    #         task_id='feature_engineering',
    #         python_callable='gs://citibikevd/feature_engineering.py')

    #feature_eng_job = PapermillOperator(
    #        task_id='feature_engineering',
    #        input_nb='gs://feature_engineering.ipynb',
    #        output_nb='/dev/null',
    #        parameters='feature-engineering-task-v{SESSION}-{VERSION}'
    #)

    #feature_eng_job = PythonOperator(
    #    python_callable=run_notebook,
    #    task_id=f'feature-engineering-task-v{SESSION}-{VERSION}'
    #)

    # create_gke_job = GKEClusterCreateOperator(
    #     task_id='gke_cluster_create',
    #     project_id=PROJECT_ID,
    #     location=ZONE,
    #     body=GKE_CLUSTER
    # )


    train_tfkeras_operator = MLEngineTrainingOperator(
            project_id=PROJECT_ID,
            task_id='tfkeras_train_job',
            job_id='tfkeras_train_job',
            package_uris=[],
            training_python_module='trainer.tfkeras.task',
            region=ZONE,
            job_dir='gs://citibikevd/diego/composertest/',
            training_args=[],
            python_version='2.7',
            runtime_version = '2.1'
    )





    # delete_gke_job = GKEClusterDeleteOperator(
    #     task_id='gke_cluster_delete',
    #     project_id=PROJECT_ID,
    #     location=ZONE,
    #     name=GKE_CLUSTER_NAME
    # )

    # Declare task dependencies
    # setup_job >> clean_job

    #create_gke_job >> delete_gke_job

    # AI Platform Operators
