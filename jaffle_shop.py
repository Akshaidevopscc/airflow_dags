from airflow import DAG, settings
from airflow.models import DagRun
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.models import TaskInstance

def clear_failed_tasks_of_another_dag(context, target_dag_id, target_dag_run_id):
    try:
        # Find the failed task instances of the target DAG run
        failed_task_instances = [
            ti for ti in context.get("task_instance_list")
            if ti.dag_id == target_dag_id and ti.execution_date == target_dag_run_id and ti.state == 'failed'
        ]

        # Clear the failed task instances
        for task_instance in failed_task_instances:
            task_instance.clear()
            print(f"Cleared failed task instance {task_instance.task_id} of DAG {target_dag_id}, run {target_dag_run_id}")
    except Exception as e:
        print(f"Error clearing failed tasks of DAG run {target_dag_run_id} in DAG {target_dag_id}: {e}")
######


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
        task_id='t0',
        on_failure_callback=lambda context: clear_failed_tasks_of_another_dag(context, "clear_upstream_task", "scheduled__2024-02-06T13:46:51.401176+00:00")
    )
    t1 = DummyOperator(
        task_id='t1',
        on_failure_callback=lambda context: clear_failed_tasks_of_another_dag(context, "clear_upstream_task", "scheduled__2024-02-06T13:46:51.401176+00:00")
    )
    t2 = DummyOperator(
        task_id='t2',
        on_failure_callback=lambda context: clear_failed_tasks_of_another_dag(context, "clear_upstream_task", "scheduled__2024-02-06T13:46:51.401176+00:00")
    )
    t3 = BashOperator(
        task_id='t3',
        bash_command='exit 123',
        on_failure_callback=lambda context: clear_failed_tasks_of_another_dag(context, "clear_upstream_task", "scheduled__2024-02-06T13:46:51.401176+00:00")
    )
    t0 >> t1 >> t2 >> t3
