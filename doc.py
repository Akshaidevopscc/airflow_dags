from pendulum import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from pathlib import Path
from airflow.operators.bash import BashOperator
from cosmos import DbtTaskGroup, RenderConfig, LoadMode
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig

AIRFLOW_USER = "airflow"
POSTGRES_TEST_PASSWORD = Variable.get("AIRFLOW_POSTGRES_TEST_PASSWORD")

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath="/appz/home/airflow/dags/dbt/jaffle_shop_akshai/profiles.yml",
)

with DAG(
    dag_id="env_test",
    start_date=datetime(2023, 11, 10),
    schedule=None,
    catchup=False,
) as dag:

    e1 = EmptyOperator(task_id="pre_dbt")

    project_path = Path("/appz/home/airflow/dags/dbt/jaffle_shop_akshai")
    dbt_executable_path = "/dbt_venv/bin/dbt"

    seeds_tg = DbtTaskGroup(
        group_id="dbt_seeds_group",
        project_config=ProjectConfig(
            dbt_project_path=project_path,
            env_vars={"AIRFLOW_POSTGRES_TEST_USER": AIRFLOW_USER, "AIRFLOW_POSTGRES_TEST_PASSWORD": POSTGRES_TEST_PASSWORD}
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
        render_config=RenderConfig(select=["path:seeds/"]),
        default_args={"retries": 2},
    )

    stg_tg = DbtTaskGroup(
        group_id="dbt_stg_group",
        project_config=ProjectConfig(
            dbt_project_path=project_path,
            env_vars={"AIRFLOW_POSTGRES_TEST_USER": AIRFLOW_USER, "AIRFLOW_POSTGRES_TEST_PASSWORD": POSTGRES_TEST_PASSWORD}
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
        render_config=RenderConfig(select=["path:models/staging/"]),
        default_args={"retries": 2},
    )

    dbt_tg = DbtTaskGroup(
        group_id="dbt_final_group",
        project_config=ProjectConfig(
            dbt_project_path=project_path,
            env_vars={"AIRFLOW_POSTGRES_TEST_USER": AIRFLOW_USER, "AIRFLOW_POSTGRES_TEST_PASSWORD": POSTGRES_TEST_PASSWORD}
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=dbt_executable_path),
        render_config=RenderConfig(exclude=["path:models/staging", "path:seeds/"]),
        default_args={"retries": 2},
    )
    
    # New task for generating dbt documentation
    dbt_generate_docs = BashOperator(
        task_id="dbt_generate_docs",
        bash_command=f"{dbt_executable_path} docs generate",
        env={
            "AIRFLOW_POSTGRES_TEST_USER": AIRFLOW_USER,
            "AIRFLOW_POSTGRES_TEST_PASSWORD": POSTGRES_TEST_PASSWORD
        },
        cwd=project_path,
    )

    dbt_serve_docs = BashOperator(
        task_id="dbt_serve_docs",
        bash_command=f"{dbt_executable_path} docs serve --port 9090 &",
        env={
            "AIRFLOW_POSTGRES_TEST_USER": AIRFLOW_USER,
            "AIRFLOW_POSTGRES_TEST_PASSWORD": POSTGRES_TEST_PASSWORD
        },
        cwd=project_path,
    )
    
    e2 = EmptyOperator(task_id="post_dbt")
    
    e1 >> seeds_tg >> stg_tg >> dbt_tg >> dbt_generate_docs >> dbt_serve_docs >> e2

