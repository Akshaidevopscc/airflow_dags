from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def verify_postgres_auth(**kwargs):
    conn_id = kwargs.get("conn_id", "postgres_default")
    hook = PostgresHook(postgres_conn_id=conn_id)
    try:
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT 1;")
        cursor.fetchone()
        cursor.close()
        conn.close()
        print("✅ PostgreSQL authentication successful.")
    except Exception as e:
        print(f"❌ PostgreSQL authentication failed: {e}")
        raise  # This makes the task (and DAG) fail

with DAG(
    dag_id="postgres_auth_check",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["postgres", "auth_check"],
) as dag:

    auth_check = PythonOperator(
        task_id="check_postgres_auth",
        python_callable=verify_postgres_auth,
        op_kwargs={"conn_id": "postgres_default"},  # Replace with your Airflow connection ID
    )

    auth_check
