from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from pathlib import Path

with DAG(
    dag_id="doc_generate",
    start_date=datetime(2023, 11, 10),
    schedule=None,
    catchup=False,
) as dag:
    
    dbt_executable_path = "/dbt_venv/bin/dbt"
    
    dbt_generate_docs_1 = BashOperator(
        task_id="dbt_generate_docs_1",
        bash_command = f"{dbt_executable_path} docs generate --target dev --project-dir /appz/home/airflow/dags/dbt/jaffle_shop_akshai --profiles-dir /appz/home/airflow/dags/dbt/jaffle_shop_akshai"
    )

    dbt_serve_docs_1 = BashOperator(
        task_id="dbt_serve_docs_1",
        bash_command = f"{dbt_executable_path} docs serve --target dev --project-dir /appz/home/airflow/dags/dbt/jaffle_shop_akshai --profiles-dir /appz/home/airflow/dags/dbt/jaffle_shop_akshai &"
    )

    dbt_generate_docs_2 = BashOperator(
        task_id="dbt_generate_docs_2",
        bash_command = f"{dbt_executable_path} docs generate --target dev --project-dir /appz/home/airflow/dags/dbt/data_engineering --profiles-dir /appz/home/airflow/dags/dbt/data_engineering"
    )

    dbt_serve_docs_2 = BashOperator(
        task_id="dbt_serve_docs_2",
        bash_command = f"{dbt_executable_path} docs serve --target dev --port 9090 --project-dir /appz/home/airflow/dags/dbt/data_engineering --profiles-dir /appz/home/airflow/dags/dbt/data_engineering &"
    )

    dbt_generate_docs_1 >> dbt_serve_docs_1 >> dbt_generate_docs_2 >> dbt_serve_docs_2
