from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.api.client.local_client import Client
from datetime import datetime, timedelta

def clear_upstream_task(context):
    execution_date = context.get("execution_date")
    client = Client(None)
    upstream_tasks = context['task'].get_direct_relatives(upstream=True)
    for task in upstream_tasks:
        cleared_instances = client.clear_task_instances(
            task_id=task.task_id,
            execution_date=execution_date,
            upstream=True,
            downstream=False,
            recursive=True,
            include_subdags=True
        )
        if cleared_instances:
            print(f"Cleared task instances for task {task.task_id}")
        else:
            print(f"Failed to clear task instances for task {task.task_id}")


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('clear_upstream_task',
         start_date=datetime(2021, 1, 1),
         max_active_runs=3,
         schedule_interval=timedelta(minutes=5),
         default_args=default_args,
         catchup=False
         ) as dag:
    t0 = DummyOperator(
        task_id='t0'
    )
    t1 = DummyOperator(
        task_id='t1'
    )
    t2 = DummyOperator(
        task_id='t2'
    )
    t3 = BashOperator(
        task_id='t3',
        bash_command='exit 123',
        on_failure_callback=clear_upstream_task
    )
    t0 >> t1 >> t2 >> t3
################
