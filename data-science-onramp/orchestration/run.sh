# To update ai platform code, run the following command in ai-platform/
# python setup.py sdist --formats=gztar

gsutil cp -r resources gs://us-east4-diego-tushar-exper-5c6c6ccc-bucket/dags

gcloud composer environments storage dags import --environment diego-tushar-experience-sequel --location us-east4 --source dag.py

echo Submitted DAG to Composer ... good luck
