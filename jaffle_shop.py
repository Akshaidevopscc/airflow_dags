from datetime import datetime
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.state import State
from cosmos import DbtTaskGroup, RenderConfig
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from pathlib import Path
class IntentionalFailureException(AirflowException):
    pass

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath="/appz/home/airflow/dags/dbt/jaffle_shop_akshai/profiles.yml",
)

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

    def fail_task():
        raise IntentionalFailureException("Intentional failure occurred in dbt_tg stage")

    fail_task_op = PythonOperator(
        task_id="fail_task_op",
        python_callable=fail_task,
    )

    try:
        fail_task_op.execute(context={})
    except IntentionalFailureException as e:
        # Find the task instance and set its state to "failed"
        task_instance = TaskInstance(task=fail_task_op, execution_date=datetime.now())
        task_instance.set_state(state=State.FAILED)

    e2 = EmptyOperator(task_id="post_dbt")

    check_and_clear_task_op = PythonOperator(
        task_id="check_and_clear_task",
        python_callable=check_and_clear_task,
    )

    e1 >> seeds_tg >> stg_tg >> dbt_tg >> e2 >> check_and_clear_task_op
