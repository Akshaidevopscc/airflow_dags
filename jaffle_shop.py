from pendulum import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from cosmos import DbtTaskGroup, RenderConfig
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

from pathlib import Path

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath="/appz/home/airflow/dags/dbt/jaffle_shop/profiles.yml"
)

with DAG(
    dag_id="jaffle_shop_new",
    start_date=datetime(2023, 11, 10),
    schedule_interval="0 0 * 1 *",
):
    e1 = EmptyOperator(task_id="pre_dbt")

    dbt_tg1 = DbtTaskGroup(
        project_config=ProjectConfig(
            Path("/appz/home/airflow/dags/dbt/jaffle_shop"),
        ),
        operator_args={"append_env": True},
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="/dbt_venv/bin/dbt",
        ),
        default_args={"retries": 2},
        task_id="dbt_group_1"
    )

    dbt_tg2 = DbtTaskGroup(
        project_config=ProjectConfig(
            Path("/appz/home/airflow/dags/dbt/jaffle_shop"),
        ),
        operator_args={"append_env": True},
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="/dbt_venv/bin/dbt",
        ),
        default_args={"retries": 2},
        task_id="dbt_group_2"
    )

    dbt_tg3 = DbtTaskGroup(
        project_config=ProjectConfig(
            Path("/appz/home/airflow/dags/dbt/jaffle_shop"),
        ),
        operator_args={"append_env": True},
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="/dbt_venv/bin/dbt",
        ),
        default_args={"retries": 2},
        task_id="dbt_group_3"
    )

    e2 = EmptyOperator(task_id="post_dbt")

    e1 >> dbt_tg1 >> dbt_tg2 >> dbt_tg3 >> e2
