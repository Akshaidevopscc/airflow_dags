from pendulum import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, RenderConfig
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from pathlib import Path
from airflow.operators.bash_operator import BashOperator
import sys
sys.path.append("/appz/home/airflow/dags/airflow_dags_akshai")
from clear_failed_task import failed_tasks

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath="/appz/home/airflow/dags/dbt/jaffle_shop_akshai/profiles.yml",
)

with DAG(
    dag_id="airflow_dags_akshai",
    start_date=datetime(2023, 11, 10),
    schedule_interval="0 0 * 1 *",
) as dag:

    e1 = EmptyOperator(task_id="pre_dbt")

    e2 = BashOperator(
        task_id='e2',
        bash_command='exit 123',
        on_failure_callback=lambda context: failed_task('clear_upstream_task', 'manual__2024-02-06T13:31:29.848852+00:00')
    )
    
    e1 >> e2
