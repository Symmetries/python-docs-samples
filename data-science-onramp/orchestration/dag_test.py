from airflow import models

def test_dag_import():
    models.Variable.set('gcs_bucket', 'citibikevd')
    models.Variable.set('gcp_project', 'data-science-onramp')
    models.Variable.set('gce_zone', 'us-east4')
    models.Variable.set('dataproc_cluster', 'data-cleaning')

    no_dag_found = True

    from . import dag as module

    for dag in vars(module).values():
        if isinstance(dag, models.DAG):
            no_dag_found = False
            dag.test_cycle()

    assert not no_dag_found
    