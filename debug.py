from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from pathlib import Path

AIRFLOW_USER = "airflow"
POSTGRES_TEST_PASSWORD = Variable.get("AIRFLOW_POSTGRES_TEST_PASSWORD")

with DAG(
    dag_id="debug",
    start_date=datetime(2023, 11, 10),
    schedule=None,
    catchup=False,
) as dag:

    project_path = Path("/appz/home/airflow/dags/dbt/jaffle_shop_akshai")
    dbt_executable_path = "/dbt_venv/bin/dbt"
    
    debug = BashOperator(
        task_id="dbt_generate_and_serve_docs",
        bash_command=f"{dbt_executable_path} pip install dbt-core==1.6.1 && {dbt_executable_path} dbt debug",
        env={
            "AIRFLOW_POSTGRES_TEST_USER": AIRFLOW_USER,
            "AIRFLOW_POSTGRES_TEST_PASSWORD": POSTGRES_TEST_PASSWORD
        },
        cwd=project_path,
    )

    debug
