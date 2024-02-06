from pendulum import datetime
from airflow import DAG
from cosmos import DbtTaskGroup, RenderConfig
from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from datetime import datetime, timedelta
from airflow.models import DagRun
from pathlib import Path

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath="/appz/home/airflow/dags/dbt/jaffle_shop_akshai/profiles.yml",
)

def clear_failed_tasks_from_other_dag(dag_id, dag_run_id):
    dagrun = DagRun.find(dag_id=dag_id, run_id=dag_run_id)
    if dagrun:
        failed_task_ids = [ti.task_id for ti in dagrun.get_task_instances() if ti.state == 'failed']
        if failed_task_ids:
            for task_id in failed_task_ids:
                dagrun.dag.clear(
                    start_date=dagrun.execution_date,
                    end_date=dagrun.execution_date,
                    dry_run=False,
                    only_failed=False,
                    only_running=False,
                    include_subdags=True,
                    include_parentdag=True,
                    task_ids=[task_id],
                )
                print(f"Cleared failed tasks for task {task_id} from DAG {dag_id} with run_id {dag_run_id}")
        else:
            print("No failed tasks found in the specified dag run.")
    else:
        print(f"No dag run found for dag_id {dag_id} and run_id {dag_run_id}.")

def clear_upstream_task(context):
    dag_id_to_clear = "airflow_dags_akshai"
    dag_run_id_to_clear = "manual__2024-02-06T12:44:50.675348+00:00"
    
    clear_failed_tasks_from_other_dag(dag_id_to_clear, dag_run_id_to_clear)

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
        on_failure_callback=clear_upstream_task
    )

    e2 = EmptyOperator(task_id="post_dbt")

    e1 >> seeds_tg >> stg_tg >> dbt_tg >> e2
