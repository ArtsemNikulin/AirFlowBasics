from datetime import timedelta, datetime
import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

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


# python callable functions
def good_morning():
    return 'Good morning!'


def good_day():
    return 'Good day!'


def good_evening():
    return 'Good evening!'


with DAG('dag_1',
         default_args=default_arguments,
         description='DAG_1',
         schedule_interval='@once',
         catchup=False,
         tags=['example']
         ) as dag_1:
    task1 = PythonOperator(task_id='morning', python_callable=good_morning)
    task2 = PythonOperator(task_id='day', python_callable=good_day)
    task3 = PythonOperator(task_id='evening', python_callable=good_evening)

task1 >> task2 >> task3
