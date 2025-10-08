import os
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def check_postgres_auth():
    host = "postgres"
    database = "postgres"
    user = os.getenv("AIRFLOW_POSTGRES_USER")
    password = os.getenv("AIRFLOW_POSTGRES_ERROR_PASSWORD")

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
        print("✅ PostgreSQL authentication successful.")
    except Exception as e:
        print(f"❌ PostgreSQL authentication failed: {e}")
        raise  # This makes the task fail (and thus the DAG fail)

with DAG(
    dag_id="dummy_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["postgres", "auth"],
) as dag:

    verify_auth = PythonOperator(
        task_id="verify_postgres_auth",
        python_callable=check_postgres_auth,
    )

    verify_auth
