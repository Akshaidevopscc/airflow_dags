from airflow import DAG
from airflow.models import DagRun, TaskInstance
from airflow.utils.session import create_session
from airflow.utils.state import State
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from cosmos import DbtTaskGroup, RenderConfig
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from pathlib import Path

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath="/appz/home/airflow/dags/dbt/jaffle_shop_akshai/profiles.yml",
)

def clear_failed_tasks(target_dag_id, target_dag_run_id):

    with create_session() as session:
        dag_run = session.query(DagRun).filter(
            DagRun.dag_id == target_dag_id,
            DagRun.run_id == target_dag_run_id
        ).first()
        
        if not dag_run:
            raise ValueError(f'DAGRun not found for DAG ID {target_dag_id} and Run ID {target_dag_run_id}')

        failed_task_instances = session.query(TaskInstance).filter(
            TaskInstance.dag_id == target_dag_id,
            TaskInstance.run_id == target_dag_run_id,
            TaskInstance.state == State.SUCCESS
        ).all()
        
        for ti in failed_task_instances:
            ti.clear()
            session.add(ti)

with DAG(
    dag_id="airflow_dags_akshai",
    start_date=datetime(2023, 11, 10),
    schedule_interval="0 0 * 1 *",
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

    clear_failed_tasks_op = PythonOperator(
        task_id="clear_failed_tasks",
        python_callable=clear_failed_tasks,
        op_kwargs={"target_dag_id": "airflow_dags_akshai", "target_dag_run_id": "scheduled__2024-01-30T00:00:00+00:00"},
    )

    e1 >> seeds_tg >> stg_tg >> dbt_tg >> e2 >> clear_failed_tasks_op
