from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import sys
sys.path.append("/appz/home/airflow/dags/airflow_dags_akshai")
from clear_failed_task import failed_tasks

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'clear_failed_task_dag',
    default_args=default_args,
    description='DAG to clear failed tasks',
    schedule_interval=timedelta(minutes=1),
)

def clear_failed_tasks_func():
    failed_tasks('snowflake_example', 'manual__2023-12-28T04:19:32.514446+00:00')

clear_failed_task = PythonOperator(
    task_id='clear_failed_task',
    python_callable=clear_failed_tasks_func,
    dag=dag,
)

clear_failed_task
