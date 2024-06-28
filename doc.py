from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from pathlib import Path

dbt_docs_url = "https://mpmathew-test-poc.03907124.lowtouch.cloud/docs/jaffle_shop/index.html"

dbt_executable_path = "/dbt_venv/bin/dbt"

with DAG(
    dag_id="doc_generate",
    start_date=datetime(2023, 11, 10),
    schedule=None,
    catchup=False,
    description="A DAG for generating and serving DBT documentation with a link to DBT Docs",
    doc_md=f"### Documentation\nHere is the link to our [DBT Documentation]({dbt_docs_url})."
) as dag:
    
    dbt_generate_docs_1 = BashOperator(
        task_id="dbt_generate_docs_1",
        bash_command=f"{dbt_executable_path} docs generate --target dev --project-dir /appz/home/airflow/dags/dbt/jaffle_shop_akshai --profiles-dir /appz/home/airflow/dags/dbt/jaffle_shop_akshai"
    )

    dbt_serve_docs_1 = BashOperator(
        task_id="dbt_serve_docs_1",
        bash_command=f"{dbt_executable_path} docs serve --target dev --project-dir /appz/home/airflow/dags/dbt/jaffle_shop_akshai --profiles-dir /appz/home/airflow/dags/dbt/jaffle_shop_akshai &"
    )

    dbt_generate_docs_1 >> dbt_serve_docs_1
##############
