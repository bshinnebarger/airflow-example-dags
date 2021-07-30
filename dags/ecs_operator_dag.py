import os

from datetime import datetime
from datetime import timedelta

from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ecs_operator import ECSOperator

DAG_NAME = 'ECSExternalTasksDag'

default_args = {
    'owner': 'Brett',
    'start_date': datetime(2020, 7, 1),
    'email': ['xyz@amazon.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

def get_ecs_operator_args(task_definition_name, task_container_name, entry_file, param):
    return dict(
        launch_type="FARGATE",
        # The name of your task as defined in ECS
        task_definition=task_definition_name,
        aws_conn_id = 'aws_default',
        region_name = os.environ['AWS_DEFAULT_REGION'],
        platform_version="1.4.0",
        # The name of your ECS cluster
        cluster=os.environ['CLUSTER'],
        network_configuration={
            'awsvpcConfiguration': {
                'securityGroups': [os.environ['SECURITY_GROUP']],
                'subnets': os.environ['SUBNETS'].split(","),
                'assignPublicIp': "DISABLED"
            }
        },
        overrides={
            'containerOverrides': [
                {
                    'name': task_container_name,
                    'command': ["python", entry_file, param]
                }
            ]
        },
        awslogs_group="FairflowExternalTaskLogs-FairflowStack",
        awslogs_stream_prefix="FairflowExternalTask/"+task_container_name
    )

odd_task_config = {
  'task_definition_name': 'BigGuys-FairflowStack',
  'task_container_name': 'BigTaskContainer',
  'entry_file': 'odd_numbers.py',
  'param': '10'
}
even_task_config = {
  'task_definition_name': 'BigGuys-FairflowStack',
  'task_container_name': 'BigTaskContainer',
  'entry_file': 'even_numbers.py',
  'param': '10'
}
numbers_task_config = {
  'task_definition_name': 'LittleGuys-FairflowStack',
  'task_container_name': 'LittleTaskContainer',
  'entry_file': 'numbers.py',
  'param': '10'
}

odd_task_args = get_ecs_operator_args(**odd_task_config)
even_task_args = get_ecs_operator_args(**even_task_config)
numbers_task_args = get_ecs_operator_args(**numbers_task_config)

with DAG(DAG_NAME,
         schedule_interval=None,
         default_args=default_args) as dag:

    start_process = DummyOperator(task_id="start_process")

    # Following tasks will get triggered from worker and runs on OnDemand Fargate Task
    #   see: https://github.com/apache/airflow/blob/8b100fcb427dc8e6f511e6ce2deddb2e04909291/airflow/providers/amazon/aws/operators/ecs.py
    odd_task = ECSOperator(task_id="odd_task", **odd_task_args)
    even_task = ECSOperator(task_id="even_task", **even_task_args)
    numbers_task = ECSOperator(task_id="numbers_task", **numbers_task_args)

    start_process >> [odd_task, even_task] >> numbers_task