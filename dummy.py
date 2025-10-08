import os
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def check_postgres_auth_wrong():
    host = "postgres"
    database = "postgres"
    user = os.getenv("AIRFLOW_POSTGRES_USER")
    password = os.getenv("REDIS_USER_PASSWD")  # wrong env
    conn = psycopg2.connect(
        host=host,
        dbname=database,
        user=user,
        password=password
    )

def check_postgres_auth_correct():
    host = "postgres"
    database = "postgres"
    user = os.getenv("AIRFLOW_POSTGRES_USER")
    password = os.getenv("AIRFLOW_POSTGRES_PASSWORD")  # correct env
    conn = psycopg2.connect(
        host=host,
        dbname=database,
        user=user,
        password=password
    )

def final_task():
    print("✅ Final task ran even if previous failed.")

with DAG(
    dag_id="postgres_auth_chain_check",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["postgres", "auth"],
) as dag:

    fail_auth = PythonOperator(
        task_id="failed_authentication",
        python_callable=check_postgres_auth_wrong,
    )

    success_auth = PythonOperator(
        task_id="success_authentication",
        python_callable=check_postgres_auth_correct,
        trigger_rule='all_done',  # runs even if previous failed
    )

    final = PythonOperator(
        task_id="final_task",
        python_callable=final_task,
        trigger_rule='all_done',  # runs regardless of prior status
    )

    fail_auth >> success_auth >> final
