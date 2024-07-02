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
    schedule_interval='@daily',
)

python_task = PythonOperator(
    task_id='python_task',
    python_callable=print_hello,
    dag=dag,
)

bash_task = BashOperator(
    task_id='bash_task',
    bash_command='echo Hello from BashOperator!',
    dag=dag,
)

python_task >> bash_task
