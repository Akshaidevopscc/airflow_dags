from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from pathlib import Path

AIRFLOW_USER = "airflow"
POSTGRES_TEST_PASSWORD = Variable.get("AIRFLOW_POSTGRES_TEST_PASSWORD")

def get_docs_path(dag_id):
    return f"/appz/home/airflow/docs/{dag_id}"

with DAG(
    dag_id="doc_generate",
    start_date=datetime(2023, 11, 10),
    schedule=None,
    catchup=False,
) as dag:

    project_path = Path("/appz/home/airflow/dags/dbt/jaffle_shop_akshai")
    dbt_executable_path = "/dbt_venv/bin/dbt"
    
    dag_id = dag.dag_id
    docs_path = get_docs_path(dag_id)
    
    dbt_generate_and_serve_docs = BashOperator(
        task_id="dbt_generate_and_serve_docs",
        bash_command=f"cd {docs_path} && {dbt_executable_path} docs generate && {dbt_executable_path} docs serve &",
        env={
            "AIRFLOW_POSTGRES_TEST_USER": AIRFLOW_USER,
            "AIRFLOW_POSTGRES_TEST_PASSWORD": POSTGRES_TEST_PASSWORD
        },
        cwd=project_path,
    )

    dbt_generate_and_serve_docs
