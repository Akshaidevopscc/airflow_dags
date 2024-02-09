from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import DagRun

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
    'clear_failed_task',
    default_args=default_args,
    description='DAG to clear failed tasks',
    schedule_interval=timedelta(minutes=1),
)

def failed_tasks(target_dag_id, target_dag_run_id):
    dagruns = DagRun.find(dag_id=target_dag_id, run_id=target_dag_run_id)
    if dagruns:
        for dagrun in dagruns:
            for ti in dagrun.get_task_instances():
                if ti.state == 'failed':
                    ti.set_state('none')

def clear_failed_tasks_func():
    failed_tasks('airflow_dags_akshai', 'manual__2024-02-09T08:22:42.445262+00:00')

clear_failed_task = PythonOperator(
    task_id='clear_failed_task',
    python_callable=clear_failed_tasks_func,
    dag=dag,
)

clear_failed_task
