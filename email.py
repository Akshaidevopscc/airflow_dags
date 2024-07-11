import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.smtp.hooks.smtp import SmtpHook

def on_failure_callback(context, SVC_NAME):
    svc = SVC_NAME
    task = context.get("task_instance").task_id
    dag = context.get("task_instance").dag_id
    ti = context.get("task_instance")
    exec_date = context.get("execution_date")
    dag_run = context.get('dag_run')
    log_url = context.get("task_instance").log_url
    msg = f"""
    SVC: {svc}
    Dag: {dag}
    Task: {task}
    DagRun: {dag_run}
    TaskInstance: {ti}
    Log Url: {log_url}
    Execution Time: {exec_date}
    """
    print(msg)

def send_smtp_email(**context):
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date'].isoformat()

    smtp_hook = SmtpHook(smtp_conn_id='smtp_primary')
    smtp_hook.get_conn()
    smtp_hook.send_email_smtp(
        to='rejith.krishnan@tcw.com', 
        subject=f'Airflow-DEV: <TaskInstance: {dag_id}.{task_id} manual__{execution_date} [failed]>', 
        html_content=f"<p>This email is sent by DAG: {dag_id}, Task: {task_id}, Execution Date: {execution_date}</p>"
    )

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email': ['rejith.krishnan@tcw.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id="DailyHealthCheck_v7",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 2, 9, tz="UTC"),
    schedule_interval="0 9 * * *",  # 1 AM PST
    description="A DAG for generating and serving DBT documentation with a link to DBT DOCS",
    tags=["SAMPLE"],
) as dag:
    e1 = EmptyOperator(task_id="pre_dbt")

    with TaskGroup('SMTP') as email_tasks:

        send_smtp_email_task = PythonOperator(
            task_id='SMTPHookCheck',
            python_callable=send_smtp_email,
            provide_context=True
        )

        email_task = EmailOperator(
            task_id='EmailOperatorCheck',
            to='rejith.krishnan@tcw.com',
            subject='Airflow-DEV: Airflow EmailOperator (Task) Check',
            html_content="Date: {{ ds }}",
        )

    e2 = EmptyOperator(task_id="post_dbt")

    e1 >> email_tasks >> e2
