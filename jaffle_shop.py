from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import TaskInstance
from airflow.utils.session import create_session
from cosmos import DbtTaskGroup, RenderConfig
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from pathlib import Path
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath="/appz/home/airflow/dags/dbt/jaffle_shop_akshai/profiles.yml",
)

def clear_upstream_tasks(context):
    execution_date = context.get("execution_date")
    clear_tasks = BashOperator(
        task_id='clear_tasks',
        bash_command=f'airflow tasks clear -s {execution_date} -t downstream_task_id -d -y <your_dag_id>'
    )
    return clear_tasks.execute(context=context)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('your_dag_id',
         start_date=datetime(2021, 1, 1),
         max_active_runs=3,
         schedule_interval=timedelta(minutes=5),
         default_args=default_args,
         catchup=False
         ) as dag:

    t0 = DummyOperator(task_id='t0')
    t1 = DummyOperator(task_id='t1')
    t2 = DummyOperator(task_id='t2')
    t3 = BashOperator(
        task_id='t3',
        bash_command='exit 123',
        on_failure_callback=clear_upstream_tasks
    )

    t0 >> t1 >> t2 >> t3
