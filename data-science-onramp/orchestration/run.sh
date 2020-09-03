# To update ai platform code, run the following command in ai-platform/
# python setup.py sdist --formats=gztar

gsutil cp -r resources gs://us-central1-leah-solo-exper-315080b4-bucket/dags

gcloud composer environments storage dags import --environment leah-solo-experience-sequel --location us-central1 --source dag.py

echo Submitted dag [$(grep 'SESSION, VERSION' < dag.py)] to Composer ... good luck
