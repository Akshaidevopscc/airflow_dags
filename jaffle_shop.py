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
from pendulum import datetime

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath="/appz/home/airflow/dags/dbt/jaffle_shop_akshai/profiles.yml",
)

def clear_upstream_tasks(context):
    execution_date = context.get("execution_date")
    clear_tasks = BashOperator(
        task_id='clear_tasks',
        bash_command=f'airflow tasks clear -s {execution_date} -t downstream_task_id -d -y airflow_dags_akshai'
    )
    return clear_tasks.execute(context=context)

with DAG(
    dag_id="airflow_dags_akshai",
    start_date=datetime(2023, 11, 10),
    schedule_interval="0 0 * 1 *",
) as dag:

    t0 = EmptyOperator(task_id='t0')
    t1 = EmptyOperator(task_id='t1')
    t2 = EmptyOperator(task_id='t2')
    t3 = PythonOperator(
        task_id='t3',
        python_callable=your_python_function,
        on_failure_callback=clear_upstream_tasks
    )

    t0 >> t1 >> t2 >> t3
####
