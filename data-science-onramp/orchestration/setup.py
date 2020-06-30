import sys

from google.cloud import bigquery

from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

BUCKET_NAME = sys.argv[1]
DATASET_NAME = 'airflow_test'
TABLE_NAME = 'test_data'

spark = SparkSession.builder.appName('airflow-induced job').getOrCreate()

schema = StructType([StructField("Words", StringType()), StructField("Total", IntegerType())])

data = [['Something', 1], ['A short sentence', 3]]

df = spark.createDataFrame(data, schema=schema)

path = f'gs://{BUCKET_NAME}/diego-tushar-experience/data.csv'

df.write.options(codec='org.apache.hadoop.io.compress.GzipCodec').csv(path, mode='overwrite')

client = bigquery.Client()
dataset_id = f'{client.project}.{DATASET_NAME}'
dataset = bigquery.Dataset(dataset_id)
dataset.location = "US"
dataset = client.create_dataset(dataset)

df.write.format('bigquery') \
        .option('table', f"{dataset_id}.{TABLE_NAME}") \
        .option("temporaryGcsBucket", BUCKET_NAME) \
        .save()

print(f'The argument is {sys.argv[1]}')
