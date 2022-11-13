from datetime import timedelta, datetime
import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from fastavro import parse_schema, writer
import csv
from collections import namedtuple



default_arguments = {
    'owner': 'artsem_test',
    'start_date': pendulum.today('UTC'),
    'end_date': datetime(2030, 12, 31),
    'depends_on_past': False,  # if yesterday's dag status is Failed, then today's will not be triggered
    'email': ['airflow@example.com'],
    'email_onfailure': False,
    'email_on_retry': False,
    'retries': 1,  # if task fails, retry 1 time
    'retray_delay': timedelta(minutes=5)  # in 5 minutes
}


path = r"/usr/local/airflow/dags/"
spark = SparkSession.builder.appName("Python Spark SQL example").config("spark.some.config.option", "some-value").getOrCreate()
sc = spark.sparkContext
df = spark.read.option("header", True).csv(path+"test.csv")

schema = {
    "namespace": "test.avro",
    "type": "record",
    "name": "test",
    "fields": [
        {"name": "region", "type": "string"},
        {"name": "anzsic_descriptor", "type": "string"},
        {"name": "gas", "type": "string"},
        {"name": "units", "type": "string"},
        {"name": "magnitude", "type": "string"},
        {"name": "year", "type": "string"},
        {"name": "data_val", "type": "string"}
    ]
}


def csv_count():
   return df.count()


def convert_csv_to_avro():
    fields = tuple(df.columns)
    forecastRecord = namedtuple('forecastRecord', fields)
    parsed_schema = parse_schema(schema)
    l = []
    with open(path+"test.csv", 'r') as data:
        data.readline()
        reader = csv.reader(data, delimiter=",")
        for records in map(forecastRecord._make, reader):
            record = dict((f, getattr(records, f)) for f in records._fields)
            l.append(record)
    with open(path+"users.avro", "wb") as fp:
        writer(fp, schema, l)

with DAG('dag_2',
         default_args=default_arguments,
         description='DAG_2',
         schedule_interval=None,
         catchup=False,
         tags=['example']
         ) as dag_2:
    task1 = PythonOperator(task_id='csv_count', python_callable=csv_count)
    task2 = PythonOperator(task_id='csv_to_avro', python_callable=convert_csv_to_avro)
    
task1 >> task2
