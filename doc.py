from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, send_from_directory
import os
from datetime import datetime

# Blueprint for the plugin
docs_blueprint = Blueprint(
    'docs_blueprint', __name__,
    url_prefix='/docs',
    template_folder='templates',
    static_folder='/appz/home/airflow/docs'
)

@docs_blueprint.route('/jaffle_shop')
def custom_static(filename):
    return send_from_directory(docs_blueprint.static_folder, filename)

class StaticDocsPlugin(AirflowPlugin):
    name = "static_docs_plugin"
    flask_blueprints = [docs_blueprint]

def run_my_dag():
    # Define your tasks here
    pass

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 18),
    'retries': 1,
}

# Instantiate the DAG
with DAG('my_static_docs_dag', 
         default_args=default_args,
         schedule_interval=None,  # Set the schedule interval as needed
         catchup=False) as dag:

    # Define tasks
    run_this = PythonOperator(
        task_id='run_my_dag_task',
        python_callable=run_my_dag
    )

# Define task dependencies
run_this


