from pendulum import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from cosmos import DbtTaskGroup, RenderConfig
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from pathlib import Path
import requests
import time
import json
import sys
sys.path.append("/appz/home/airflow/dags/airflow_dags_akshai")
from clear_task import task_clear
import PRO

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath="/appz/home/airflow/dags/dbt/jaffle_shop_akshai/profiles.yml",
)

def check_and_clear_task():
    def check_dag_status(dag_id, dag_run_id, profile):
        cred_path = f"{profile}.json"
        try:
            with open(cred_path) as file:
                credentials = json.load(file)
        except Exception as e:
            print(f"Error: {e}")
            print("Credentials not found.")
            return

        username = credentials["Username"]
        password = credentials["Password"]
        domain = credentials["Domain"]

        uri = f"https://{domain}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"

        headers = {
            "Content-Type": "application/json"
        }

        response = requests.get(uri, auth=(username, password), headers=headers)

        if response.status_code == 200:
            dag_run_details = response.json()
            return dag_run_details.get("state")
        else:
            print(f"Failed to retrieve DAG Run details. Status Code: {response.status_code}")
            return None

    while True:
        dag_run_status = check_dag_status("airflow_dags_akshai", "scheduled__2024-01-30T00:00:00+00:00", "PRO")
        if dag_run_status in ["running", "success"]:
            print("DAG run completed successfully.")
            break
            
        elif dag_run_status == "failed":
            print("DAG run failed. Initiating task clearing...")
            task_clear(profile="PRO", Dag="airflow_dags_akshai", dag_run_id="scheduled__2024-01-29T00:00:00+00:00", task_ids=["pre_dbt", "dbt_seeds_group", "dbt_final_group", "post_dbt"])
            break

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

    check_and_clear_task_op = PythonOperator(
        task_id="check_and_clear_task",
        python_callable=check_and_clear_task,
    )

    e1 >> seeds_tg >> stg_tg >> dbt_tg >> e2 >> check_and_clear_task_op
