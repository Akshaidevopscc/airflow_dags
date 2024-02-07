from pendulum import datetime
from airflow import DAG
from cosmos import DbtTaskGroup, RenderConfig
from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from datetime import datetime, timedelta
from pathlib import Path
from airflow.models import DagRun

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath="/appz/home/airflow/dags/dbt/jaffle_shop_akshai/profiles.yml",
)

def clear_failed_tasks(target_dag_id, target_dag_run_id):
    dagruns = DagRun.find(dag_id=target_dag_id, run_id=target_dag_run_id)
    if dagruns:
        for dagrun in dagruns:
            for ti in dagrun.get_task_instances():
                if ti.state == 'failed':
                    ti.set_state('none')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('airflow_dags_akshai',
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
        on_failure_callback=lambda context: clear_failed_tasks('airflow_dags_akshai', 'manual__2024-02-06T10:03:37.119589+00:00')
    )

    e2 = EmptyOperator(task_id="post_dbt")

    e1 >> seeds_tg >> stg_tg >> dbt_tg >> e2
