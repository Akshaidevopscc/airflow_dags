from pendulum import datetime
from airflow import DAG
from cosmos import DbtTaskGroup, RenderConfig
from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from datetime import datetime, timedelta
from pathlib import Path

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath="/appz/home/airflow/dags/dbt/jaffle_shop_akshai/profiles.yml",
)

def clear_upstream_task(context):
    execution_date = context.get("execution_date")
    dag = context['dag']
    task_instance = context['task_instance']
    upstream_task_ids = dag.get_task(task_instance.task_id).upstream_task_ids
    print(upstream_task_ids)
    for task_id in upstream_task_ids:
        dag.clear(
            start_date=execution_date,
            end_date=execution_date,
            dry_run=False,
            only_failed=False,
            only_running=False,
            include_subdags=True,
            include_parentdag=True,
            task_ids=[task_id],
        )
        print("Cleared upstream tasks for task {}".format(task_id))
        task_instance.xcom_push(key=f'{task_id}_status', value='no_status')
        print("****************************************************************************************************")

with DAG('clear_upstream_task',
         start_date=datetime(2021, 1, 1),
         schedule_interval="0 0 * 1 *",
         max_active_runs=3,
         default_args=default_args,
         catchup=False
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

    dbt_tg = BashOperator(
        task_id="dbt_final_group",
        bash_command='exit 123',
        on_failure_callback=clear_upstream_task
    )

    e2 = EmptyOperator(task_id="post_dbt")

    e1 >> seeds_tg >> stg_tg >> dbt_tg >> e2
