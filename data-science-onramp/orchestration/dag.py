import datetime

import airflow
from airflow.operators import bash_operator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

REGION = "us-east4"
PYSPARK_JOB = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {"main_python_file_uri": }, # TODO TODO TODO
}
PROJECT_ID = "data-science-onramp"

YESTERDAY = datetime.datetime.now() - datetime.timedelta(minutes=1)

default_args = {
    'owner': 'Composer Example',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': YESTERDAY,
}

pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task",
        job=PYSPARK_JOB,
        location=REGION,
        project_id=PROJECT_ID
)

with airflow.DAG(
        'diego-tushar-trial',
        'catchup=False',
        default_args=default_args,
        schedule_interval=datetime.timedelta(minutes=1)) as dag:

    # Submit a job to the clustter

    # Print the dag_run id from the Airflow logs
    print_dag_run_conf = bash_operator.BashOperator(
            task_id='print_dag_run_conf',
            bash_command='echo the dag run is {{ dag_run.id }}')
