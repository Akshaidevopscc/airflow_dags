import os
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def check_postgres_auth_wrong():
    host = "postgres"
    database = "postgres"
    user = os.getenv("AIRFLOW_POSTGRES_USER")
    password = os.getenv("REDIS_USER_PASSWD")  # wrong password env

    try:
        conn = psycopg2.connect(
            host=host,
            dbname=database,
            user=user,
            password=password
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1;")
        cursor.fetchone()
        cursor.close()
        conn.close()
        print("✅ Authentication unexpectedly succeeded (wrong password).")
    except Exception as e:
        print(f"❌ PostgreSQL authentication failed (expected): {e}")
        raise

def check_postgres_auth_correct():
    host = "postgres"
    database = "postgres"
    user = os.getenv("AIRFLOW_POSTGRES_USER")
    password = os.getenv("AIRFLOW_POSTGRES_PASSWORD")  # correct password env

    try:
        conn = psycopg2.connect(
            host=host,
            dbname=database,
            user=user,
            password=password
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1;")
        cursor.fetchone()
        cursor.close()
        conn.close()
        print("✅ PostgreSQL authentication successful (correct credentials).")
    except Exception as e:
        print(f"❌ PostgreSQL authentication failed: {e}")
        raise

with DAG(
    dag_id="postgres_auth_dual_check",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["postgres", "auth"],
) as dag:

    verify_auth_fail = PythonOperator(
        task_id="verify_postgres_auth_fail",
        python_callable=check_postgres_auth_wrong,
    )

    verify_auth_success = PythonOperator(
        task_id="verify_postgres_auth_success",
        python_callable=check_postgres_auth_correct,
    )

    verify_auth_fail >> verify_auth_success
