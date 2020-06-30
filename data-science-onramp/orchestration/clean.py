import sys

from google.cloud import storage

from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

BUCKET_NAME = sys.argv[1]
PROJECT_ID = sys.argv[2]
TABLE = f'{PROJECT_ID}.airflow_test.test_data'

spark = SparkSession.builder.appName('airflow-induced job').getOrCreate()

try:
    df = spark.read.format('bigquery').option('table', TABLE).load()
except Py4JJavaError:
    print(f'{TABLE} does not exist.')
    raise 

df.show()

path = f'gs://{BUCKET_NAME}/diego-tushar-experience/data.csv'

df.write.options(codec='org.apache.hadoop.io.compress.GzipCodec').csv(path, mode='overwrite')
