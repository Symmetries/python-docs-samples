from airflow import models

def test_dag_import():
    """Test that the DAG file can be imported

    This tests that the module contains a valid DAG,
    that is, a DAG with no cycles.
    """
    
    # Set necessary Airflow variables
    models.Variable.set('gcs_bucket', 'citibikevd')
    models.Variable.set('gcp_project', 'data-science-onramp')
    models.Variable.set('gce_zone', 'us-east4')
    models.Variable.set('dataproc_cluster', 'data-cleaning')

    no_dag_found = True

    from . import dag as module

    for dag in vars(module).values():
        if isinstance(dag, models.DAG):
            no_dag_found = False
            # Throw exception if DAG has a cycle
            dag.test_cycle()

    assert not no_dag_found, "No DAG was found in module"
    
