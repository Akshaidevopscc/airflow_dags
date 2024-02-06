from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

def clear_upstream_task(context):
    execution_date = context.get("execution_date")
    dag = context['dag']
    task_instance = context['task_instance']
    upstream_task_ids = dag.get_task(task_instance.task_id).upstream_task_ids
    print('**********************************************************************************')
    print(upstream_task_ids)
    for task_id in upstream_task_ids:
        dag.clear(
            start_date=execution_date,
            end_date=execution_date,
            dry_run=False,
            only_failed=False,
            only_running=False,
            include_subdags=True,
            include_parentdag=True,
            task_ids=[task_id],
        )
        print('**********************************************************************************')
        print(task_id)
        print("Cleared upstream tasks for task {}".format(task_id))

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
        task_id='t0',
        on_failure_callback=clear_upstream_task
    )
    t1 = BashOperator(
        task_id='t1',
        bash_command='exit 123',
        on_failure_callback=clear_upstream_task
    )
    t2 = DummyOperator(
        task_id='t2',
        on_failure_callback=clear_upstream_task
    )
    t3 = DummyOperator(
        task_id='t3',
        on_failure_callback=clear_upstream_task
    )
    t0 >> t1 >> t2 >> t3
