from pendulum import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig

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
) as dag:
    # Pre-DBT tasks
    pre_dbt = EmptyOperator(task_id="pre_dbt")

    # Seeds Task Group
    seeds_tg = DbtTaskGroup(
        group_id="seeds_task_group",
        project_config=ProjectConfig(Path("/appz/home/airflow/dags/dbt/jaffle_shop")),
        operator_args={"append_env": True},
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="/dbt_venv/bin/dbt"),
        default_args={"retries": 2},
        sql_files=["seeds/raw_customers.sql", "seeds/raw_orders.sql", "seeds/raw_payments.sql"],
    )

    # Staging Task Group
    staging_tg = DbtTaskGroup(
        group_id="staging_task_group",
        project_config=ProjectConfig(Path("/appz/home/airflow/dags/dbt/jaffle_shop")),
        operator_args={"append_env": True},
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="/dbt_venv/bin/dbt"),
        default_args={"retries": 2},
        sql_files=["staging/stg_customers.sql", "staging/stg_orders.sql", "staging/stg_payments.sql"],
    )

    # Final Transformation Task Group
    final_tg = DbtTaskGroup(
        group_id="final_transformation_task_group",
        project_config=ProjectConfig(Path("/appz/home/airflow/dags/dbt/jaffle_shop")),
        operator_args={"append_env": True},
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="/dbt_venv/bin/dbt"),
        default_args={"retries": 2},
        sql_files=["models/customers.sql", "models/orders.sql"],
    )

    # Post-DBT tasks
    post_dbt = EmptyOperator(task_id="post_dbt")

    # Define task dependencies
    pre_dbt >> seeds_tg >> staging_tg >> final_tg >> post_dbt
#
