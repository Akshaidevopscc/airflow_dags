from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from flask import Flask, Blueprint, send_from_directory
from datetime import datetime

# Blueprint for the plugin
docs_blueprint = Blueprint(
    'docs_blueprint', __name__,
    url_prefix='/docs/jaffle_shop',
    template_folder='templates',
    static_folder='/appz/home/airflow/docs/jaffle_shop'
)

@docs_blueprint.route('/')
def serve_index():
    return send_from_directory(docs_blueprint.static_folder, 'index.html')

@docs_blueprint.route('/<path:filename>')
def custom_static(filename):
    return send_from_directory(docs_blueprint.static_folder, filename)

class StaticDocsPlugin:
    def __init__(self):
        self.app = Flask(__name__)
        self.app.register_blueprint(docs_blueprint)

def serve_static_files():
    """
    Function to serve static files using Flask Blueprint.
    This function is used as the callable for the PythonOperator.
    """
    static_docs_plugin = StaticDocsPlugin()
    static_docs_plugin.app.run(host='0.0.0.0', port=9090, debug=False)  # Adjust port as needed

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

    # Define task to serve static files
    serve_static_task = PythonOperator(
        task_id='serve_static_files_task',
        python_callable=serve_static_files
    )

# Define task dependencies
serve_static_task
