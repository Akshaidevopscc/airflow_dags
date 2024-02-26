from pendulum import datetime
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.models import Variable

AIRFLOW_USER = "airflow"
POSTGRES_TEST_PASSWORD = Variable.get("AIRFLOW_POSTGRES_TEST_PASSWORD")

PATH_TO_DBT_PROJECT = "/appz/home/airflow/dags/dbt/jaffle_shop"
PATH_TO_DBT_VENV = "/dbt_venv/bin/dbt"

@dag(
    start_date=datetime(2023, 3, 23),
    schedule_interval=None,
    catchup=False,
)
def simple_dbt_dag():
    dbt_generate_docs = BashOperator(
        task_id="dbt_generate_docs",
        bash_command=f"{PATH_TO_DBT_VENV} docs generate",
        env={
            "PATH_TO_DBT_VENV": PATH_TO_DBT_VENV,
            "AIRFLOW_POSTGRES_TEST_USER": AIRFLOW_USER,
            "AIRFLOW_POSTGRES_TEST_PASSWORD": POSTGRES_TEST_PASSWORD
        },
        cwd=PATH_TO_DBT_PROJECT,
    )

    dbt_serve_docs = BashOperator(
        task_id="dbt_serve_docs",
        bash_command=f"{PATH_TO_DBT_VENV} docs serve",
        env={
            "PATH_TO_DBT_VENV": PATH_TO_DBT_VENV,
            "AIRFLOW_POSTGRES_TEST_USER": AIRFLOW_USER,
            "AIRFLOW_POSTGRES_TEST_PASSWORD": POSTGRES_TEST_PASSWORD
        },
        cwd=PATH_TO_DBT_PROJECT,
    )

    dbt_generate_docs >> dbt_serve_docs

simple_dbt_dag()
