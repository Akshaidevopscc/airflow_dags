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

    project_path = Path("/appz/home/airflow/dags/dbt/data_engineering")
    dbt_executable_path = "/dbt_venv/bin/dbt"
    
    dbt_generate_docs = BashOperator(
        task_id="dbt_generate_docs",
        bash_command = f"{dbt_executable_path} docs generate --target dev --project-dir /appz/home/airflow/dags/dbt/data_engineering/nyc_parking_violations/ --profiles-dir /appz/home/airflow/dags/dbt/data_engineering/nyc_parking_violations/"
    )

    dbt_generate_docs
