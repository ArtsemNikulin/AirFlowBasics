from datetime import timedelta, datetime
import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from fastavro import parse_schema, writer
import csv
from collections import namedtuple
import pandas as pd



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
df = pd.read_csv(path+'test.csv')

def pivot():
    table = pd.pivot_table(df, values='data_val', index=['region', 'year'])
    table.to_csv(path+"pivot_table.csv")
    return table



with DAG('dag_3',
         default_args=default_arguments,
         description='DAG_3',
         schedule_interval=None,
         catchup=False,
         tags=['example']
         ) as dag_3:
    task1 = PythonOperator(task_id='pivot', python_callable=pivot)

    
task1
