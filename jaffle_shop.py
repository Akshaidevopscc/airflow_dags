from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


def clear_upstream_task(context):
    execution_date = context.get("execution_date")
    clear_tasks = BashOperator(
        task_id='clear_tasks',
        bash_command=f'airflow tasks clear -s {execution_date}  -t t2 -d -y clear_upstream_task'
    )
    return clear_tasks.execute(context=context)


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
        #retries=1,
        on_failure_callback=clear_upstream_task
    )

    t0 >> t1 >> t2 >> t3
################################################
