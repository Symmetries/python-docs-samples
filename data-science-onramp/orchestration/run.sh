#gsutil cp setup.py gs://citibikevd/diego-tushar-experience/
#gsutil cp clean.py gs://citibikevd/diego-tushar-experience/

gcloud composer environments storage dags import --environment diego-tushar-experience-sequel --location us-east4 --source dag.py

echo Submitted DAG to Composer ... good luck