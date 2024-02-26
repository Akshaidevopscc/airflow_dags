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
    tags=['dbt']  
)
def simple_dbt_dag():
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{PATH_TO_DBT_VENV} run",
        env={
            "PATH_TO_DBT_VENV": PATH_TO_DBT_VENV,
            "AIRFLOW_POSTGRES_TEST_USER": AIRFLOW_USER,
            "AIRFLOW_POSTGRES_TEST_PASSWORD": POSTGRES_TEST_PASSWORD
        },
        cwd=PATH_TO_DBT_PROJECT,
    )

simple_dbt_dag()
