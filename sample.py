from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

def print_hello():
    return 'Hello from PythonOperator!'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'simple_dag',
    default_args=default_args,
    schedule_interval=None,  # No schedule, run manually
)

bash_task = BashOperator(
    task_id='bash_task',
    bash_command='exit 1',  # This will make the task fail
    dag=dag,
)

python_task = PythonOperator(
    task_id='python_task',
    python_callable=print_hello,
    trigger_rule='all_done',  # This ensures the task runs even if the previous task fails
    dag=dag,
)

bash_task >> python_task


