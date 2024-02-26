from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from cosmos import DbtTaskGroup, RenderConfig
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from pathlib import Path

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath="/appz/home/airflow/dags/dbt/jaffle_shop_akshai/profiles.yml",
)

with DAG(
    dag_id="doc",
    start_date=datetime(2023, 11, 10),
    schedule=None,
    catchup=False,
) as dag:

    e1 = BashOperator(
        task_id="pre_dbt",
        bash_command="echo 'Pre-DBT tasks'",
    )

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

    dbt_doc_generate = BashOperator(
        task_id="dbt_doc_generate",
        bash_command="/dbt_venv/bin/dbt docs generate",
    )

    dbt_doc_serve = BashOperator(
        task_id="dbt_doc_serve",
        bash_command="/dbt_venv/bin/dbt docs serve",
    )

    e2 = BashOperator(
        task_id="post_dbt",
        bash_command="echo 'Post-DBT tasks'",
    )

    e1 >> seeds_tg >> stg_tg >> dbt_tg >> dbt_doc_generate >> dbt_doc_serve >> e2
