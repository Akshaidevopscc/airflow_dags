from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator  
from airflow.models import Variable

PATH_TO_DBT_PROJECT = "/appz/home/airflow/dags/dbt/jaffle_shop"
PATH_TO_DBT_VENV = "/dbt_venv/bin/dbt"

AIRFLOW_USER = "airflow"
POSTGRES_TEST_PASSWORD = Variable.get("AIRFLOW_POSTGRES_TEST_PASSWORD")

with DAG(
    dag_id="doc",
    start_date=datetime(2023, 11, 10),
    schedule_interval=None, 
    catchup=False,
) as dag:

    create_project_dir = BashOperator(
        task_id="create_project_dir",
        bash_command=f"mkdir -p {PATH_TO_DBT_PROJECT}",
    )

    dbt_init = BashOperator(
        task_id="dbt_init",
        bash_command=f"{PATH_TO_DBT_VENV} init .",
        env={
            "PATH_TO_DBT_VENV": PATH_TO_DBT_VENV,
        },
        cwd=PATH_TO_DBT_PROJECT,
    )

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
        bash_command=f"{PATH_TO_DBT_VENV} docs serve --port 9090 &",
        env={
            "PATH_TO_DBT_VENV": PATH_TO_DBT_VENV,
            "AIRFLOW_POSTGRES_TEST_USER": AIRFLOW_USER,
            "AIRFLOW_POSTGRES_TEST_PASSWORD": POSTGRES_TEST_PASSWORD
        },
        cwd=PATH_TO_DBT_PROJECT,
    )

    create_project_dir >> dbt_init >> dbt_generate_docs >> dbt_serve_docs
