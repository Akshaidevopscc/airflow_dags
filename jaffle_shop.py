import sys
from pendulum import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, RenderConfig
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from pathlib import Path
from clear_task import task_clear

sys.path.append("/home/ec2-user/airflow/dags/airflow_dags_akshai")

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath="/appz/home/airflow/dags/dbt/jaffle_shop_akshai/profiles.yml",
)

def on_failure_callback(context):
    task_clear(profile="PRO", Dag="airflow_dags_akshai", dag_run_id=context['dag_run'].run_id, task_ids=["pre_dbt", "dbt_seeds_group", "dbt_stg_group", "dbt_final_group", "post_dbt"])

with DAG(
    dag_id="airflow_dags_akshai",
    start_date=datetime(2023, 11, 10),
    schedule_interval="0 0 * 1 *",
    on_failure_callback=on_failure_callback,
) as dag:

    e1 = EmptyOperator(task_id="pre_dbt")

    seeds_tg = DbtTaskGroup(
        group_id="dbt_seeds_group",
        project_config=ProjectConfig(Path("/appz/home/airflow/dags/dbt/jaffle_shop_akshai")),
        operator_args={"append_env": True},
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="/dbt_venv/bin/dbt"),
        render_config=RenderConfig(select=["path:seeds/"]),
        default_args={"retries": 2},
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
