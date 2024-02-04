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

def clear_all_tasks(context):
    execution_date = context.get("execution_date")
    dag_id = context["dag"].dag_id
    task_instances = (
        session.query(TaskInstance)
        .filter(TaskInstance.dag_id == dag_id, TaskInstance.execution_date == execution_date)
        .all()
    )
    for task_instance in task_instances:
        task_instance.clear()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    "dry_run": False,
    "only_failed": True,
    "only_running": False,
    "include_subdags": True,
    "include_parentdag": True,
    "reset_dag_runs": True,
    "include_upstream": True,
    "include_downstream": True,
    "include_future": False,
    "include_past": False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'on_failure_callback': clear_all_tasks,
}

with DAG(
    dag_id="airflow_dags_akshai",
    start_date=datetime(2023, 11, 10),
    schedule_interval="0 0 * 1 *",
    default_args=default_args,
    catchup=False
) as dag:

    e1 = EmptyOperator(task_id="pre_dbt")

    seeds_tg = BashOperator(
    task_id="seeds_tg",
    bash_command="exit 1", 
    )

    stg_tg = DbtTaskGroup(
        group_id="dbt_stg_group",
        project_config=ProjectConfig(Path("/appz/home/airflow/dags/dbt/jaffle_shop_akshai")),
        operator_args={"append_env": True},
        profile_config=profile_config,  
        execution_config=ExecutionConfig(dbt_executable_path="/dbt_venv/bin/dbt"),
        render_config=RenderConfig(select=["path:models/staging/"]),
        default_args={"retries": 2},
    )

    dbt_tg = DbtTaskGroup(
        group_id="dbt_final_group",
        project_config=ProjectConfig(Path("/appz/home/airflow/dags/dbt/jaffle_shop_akshai")),
        operator_args={"append_env": True},
        profile_config=profile_config, 
        execution_config=ExecutionConfig(dbt_executable_path="/dbt_venv/bin/dbt"),
        render_config=RenderConfig(exclude=["path:models/staging", "path:seeds/"]),
        default_args={"retries": 2},
    )

    e2 = EmptyOperator(task_id="post_dbt")

    e1 >> seeds_tg >> stg_tg >> dbt_tg >> e2
