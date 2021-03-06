from datetime import datetime
from datetime import timedelta

from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator

DAG_NAME = 'SynchronizeRepository'

default_args = {
    'owner': 'Brett',
    'start_date': datetime(2020, 7, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(DAG_NAME,
          schedule_interval=None,
          default_args=default_args)

start_process = DummyOperator(task_id="start_process", dag=dag)

sync_repo_task = BashOperator(
    task_id = 'synchronize_repo',
    bash_command = 'source /sync_repo.sh; sync_repo;',
    dag=dag
)

start_process >> sync_repo_task