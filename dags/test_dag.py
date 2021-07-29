from datetime import datetime
from datetime import timedelta

from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from src.whats_it_all_about import meaning_of_life

DAG_NAME = 'WhatsItAboutDag'

default_args = {
    'owner': 'Brett',
    'start_date': datetime(2020, 7, 1),
    'email': ['xyz@amazon.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(DAG_NAME,
          schedule_interval=None,
          default_args=default_args)

start_process = DummyOperator(task_id="start_process", dag=dag)

on_worker_task = PythonOperator(
    task_id='runs_on_worker',
    python_callable=meaning_of_life,
    dag=dag,
)

start_process >> on_worker_task
