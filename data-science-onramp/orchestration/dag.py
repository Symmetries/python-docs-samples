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
#from airflow.operators.python_operator import PythonOperator
#from airflow.operators.bash_operator import BashOperator
#from airflow.operators.papermill_operator import PapermillOperator
#from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
#from airflow.contrib.operators.mlengine_operator import MLEngineTrainingOperator, MLEngineVersionOperator
# from airflow.providers.google.cloud.hooks.dataproc import DataprocSubmitJobOperator

# GKEPodOperator should be replaced by GKEStartPodOperator when it is supported
from airflow.contrib.operators.gcp_container_operator import GKEPodOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import GKECreateClusterOperator, GKEDeleteClusterOperator #, GKEStartPodOperator 
from airflow.providers.google.cloud.operators.mlengine import MLEngineCreateModelOperator, MLEngineCreateVersionOperator

from google.cloud.container_v1.types import Cluster, NodePool, NodeConfig
#import pandas as pd
import uuid

SESSION, VERSION = 27, 0

# Get Airflow varibles
PROJECT_ID = models.Variable.get('gcp_project')
BUCKET_NAME = models.Variable.get('gcs_bucket')
REGION = models.Variable.get('gce_region')
ZONE = models.Variable.get('gce_zone')
DATAPROC_CLUSTER_NAME = models.Variable.get('dataproc_cluster')
GKE_CLUSTER_NAME = f'tiego'

# Set AI Platform variables
AIPLATFORM_JOB_DIR = 'gs://citibikevd/aiplatform/output'
TFKERAS_MODEL = f"tfkeras_model_tiego" #{str(uuid.uuid4()).replace('-', '_')}

node_config = NodeConfig(machine_type='n1-standard-16')
node_pool = NodePool(initial_node_count=1, config=node_config)
GKE_CLUSTER = Cluster(name=GKE_CLUSTER_NAME, initial_node_count=1, node_config=node_config)

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least five minutes
    'retries': 0,
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

    create_gke_job = GKECreateClusterOperator(
        task_id='gke_cluster_create',
        project_id=PROJECT_ID,
        location=ZONE,
        body=GKE_CLUSTER
    )

    
    start_gke_pod = GKEPodOperator(
        task_id='gke_start_pod',
        project_id=PROJECT_ID,
        location=ZONE,
        cluster_name=GKE_CLUSTER_NAME,
        name='feature-engineering',
        namespace='default',
        image='gcr.io/data-science-onramp/tiego'
    )
    
    
    delete_gke_job = GKEDeleteClusterOperator(
        task_id='gke_cluster_delete',
        project_id=PROJECT_ID,
        location=ZONE,
        name=GKE_CLUSTER_NAME
    )
    


    # train_sklearn_job = MLEngineTrainingOperator(
    #     task_id='sklearn_train_job',
    #     project_id=PROJECT_ID,
    #     job_id=f'sklearn_train_job_{uuid.uuid4()}',
    #     package_uris='gs://citibikevd/aiplatform/trainer-0.1.tar.gz',
    #     training_python_module='trainer.sklearn_model.task',
    #     training_args=[],
    #     region=REGION,
    #     job_dir=AIPLATFORM_JOB_DIR,
    #     runtime_version = '2.1',
    #     python_version='3.7'
    # )

    '''
    train_tfkeras_job = MLEngineTrainingOperator(
        task_id='tfkeras_train_job',
        project_id=PROJECT_ID,
        job_id=f'tfkeras_train_job_{uuid.uuid4()}',
        package_uris='gs://citibikevd/aiplatform/trainer-0.1.tar.gz',
        training_python_module='trainer.tfkeras_model.task',
        training_args=[],
        region=REGION,
        job_dir=AIPLATFORM_JOB_DIR,
        runtime_version = '2.1',
        python_version='3.7'
    )

    create_tfkeras_model = MLEngineCreateModelOperator(
       task_id="create-tfkeras-model",
        project_id=PROJECT_ID,
        model={
            "name": TFKERAS_MODEL
        }
    )

    create_tfkeras_version = MLEngineCreateVersionOperator(
        task_id="create-tfkeras-version",
        project_id=PROJECT_ID,
        model_name=TFKERAS_MODEL,
        version={
            "name": "v1",
            "description": "tk-keras model version 1",
            "deployment_uri": f'{AIPLATFORM_JOB_DIR}/keras_export/',
            "runtime_version": "2.1",
            "framework": "TENSORFLOW",
            "pythonVersion": "3.7"
        }
    )
    '''





    # Declare task dependencies
    #setup_job >> clean_job
    #create_tfkeras_model >> create_tfkeras_version
    #train_tfkeras_job >> create_tfkeras_version

    create_gke_job >> start_gke_pod >> delete_gke_job
