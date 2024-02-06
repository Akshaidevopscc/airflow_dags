from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import DagRun, TaskInstance
from airflow.utils.session import create_session
from airflow.utils.state import State
from datetime import datetime, timedelta

def clear_failed_tasks(target_dag_id, target_dag_run_id):
    with create_session() as session:
        dag_run = session.query(DagRun).filter(
            DagRun.dag_id == target_dag_id,
            DagRun.run_id == target_dag_run_id
        ).first()
        
        if not dag_run:
            raise ValueError(f'DAGRun not found for DAG ID {target_dag_id} and Run ID {target_dag_run_id}')

        for ti in dag_run.get_task_instances():
            if ti.state == State.FAILED:
                ti.state = State.NONE
                session.commit()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('clear_upstream_task',
         start_date=datetime(2021, 1, 1),
         max_active_runs=3,
         default_args=default_args,
         catchup=False
         ) as dag:
    t0 = DummyOperator(
        task_id='t0'
    )
    t1 = DummyOperator(
        task_id='t1'
    )
    t2 = DummyOperator(
        task_id='t2'
    )
    t3 = BashOperator(
        task_id='t3',
        bash_command='exit 123',
        on_failure_callback=lambda context: clear_failed_tasks('clear_upstream_task', 'scheduled__2024-02-06T13:46:51.401176+00:00')
    )
    t0 >> t1 >> t2 >> t3
