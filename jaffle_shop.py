from pendulum import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, RenderConfig
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from airflow.operators.python_operator import PythonOperator
from pathlib import Path

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath="/appz/home/airflow/dags/dbt/jaffle_shop_akshai/profiles.yml",
)

def clear_upstream_task(context):
    execution_date = context.get("execution_date")
    dag = context['dag']
    task_instance = context['task_instance']
    upstream_task_ids = dag.get_task(task_instance.task_id).upstream_task_ids
    print(upstream_task_ids)
    for task_id in upstream_task_ids:
        dag.clear(
            start_date=execution_date,
            end_date=execution_date,
            dry_run=False,
            only_failed=False,
            only_running=False,
            include_subdags=True,
            include_parentdag=True,
            task_ids=[task_id],
        )
        print("Cleared upstream tasks for task {}".format(task_id))
        # Change the status of the cleared task to 'no_status'
        task_instance.xcom_push(key=f'{task_id}_status', value='no_status')
        print("****************************************************************************************************")

def dbt_task_group(dag, group_id, render_config_select, retries):
    return DbtTaskGroup(
        group_id=group_id,
        project_config=ProjectConfig(Path("/appz/home/airflow/dags/dbt/jaffle_shop_akshai")),
        operator_args={"append_env": True},
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="/dbt_venv/bin/dbt"),
        render_config=RenderConfig(select=[render_config_select]),
        default_args={"retries": retries},
    )

with DAG(
    dag_id="airflow_dags_akshai",
    start_date=datetime(2023, 11, 10),
    schedule_interval="0 0 * 1 *",
) as dag:

    e1 = EmptyOperator(task_id="pre_dbt")

    seeds_tg = dbt_task_group(dag, "dbt_seeds_group", "path:seeds/", 2)
    stg_tg = dbt_task_group(dag, "dbt_stg_group", "path:models/staging/", 2)
    dbt_tg = dbt_task_group(dag, "dbt_final_group", "", 2)

    e2 = EmptyOperator(task_id="post_dbt")

    clear_upstream = PythonOperator(
        task_id='clear_upstream_task',
        python_callable=clear_upstream_task,
        provide_context=True
    )

    e1 >> seeds_tg >> stg_tg >> dbt_tg >> e2
    seeds_tg >> clear_upstream
    stg_tg >> clear_upstream
    dbt_tg >> clear_upstream
