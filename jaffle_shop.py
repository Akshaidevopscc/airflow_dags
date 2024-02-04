from pendulum import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, RenderConfig
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from pathlib import Path
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

# Define the function to clear upstream tasks
def clear_upstream_task(context):
    execution_date = context.get("execution_date")
    clear_tasks = BashOperator(
        task_id='clear_tasks',
        bash_command=f'airflow tasks clear -s {execution_date}  -t seeds_tg -d -y airflow_dags_akshai'
    )
    return clear_tasks.execute(context=context)

# Define profile configuration
profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath="/appz/home/airflow/dags/dbt/jaffle_shop_akshai/profiles.yml",
)

# Define default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,  # Set retries to 2 for all tasks
    'retry_delay': timedelta(seconds=5)
}

# Define the DAG
with DAG(
    dag_id="airflow_dags_akshai",
    start_date=datetime(2023, 11, 10),
    schedule_interval="0 0 * 1 *",
    default_args=default_args,
    catchup=False
) as dag:

    # Define operators
    e1 = EmptyOperator(task_id="pre_dbt")
    seeds_tg = BashOperator(
        task_id="seeds_tg",
        bash_command="exit 1",  # This task will fail for sure
        on_failure_callback=clear_upstream_task  # Call the clear_upstream_task function upon failure
    )

    stg_tg = DbtTaskGroup(
        group_id="dbt_stg_group",
        project_config=ProjectConfig(Path("/appz/home/airflow/dags/dbt/jaffle_shop_akshai")),
        operator_args={"append_env": True},
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="/dbt_venv/bin/dbt"),
        render_config=RenderConfig(select=["path:models/staging/"]),
    )

    dbt_tg = DbtTaskGroup(
        group_id="dbt_final_group",
        project_config=ProjectConfig(Path("/appz/home/airflow/dags/dbt/jaffle_shop_akshai")),
        operator_args={"append_env": True},
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="/dbt_venv/bin/dbt"),
        render_config=RenderConfig(exclude=["path:models/staging", "path:seeds/"]),
    )

    e2 = EmptyOperator(task_id="post_dbt")

    # Define task dependencies
    e1 >> seeds_tg >> stg_tg >> dbt_tg >> e2
