from pendulum import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="jaffle_shop",
    start_date=datetime(2023, 11, 10),
    schedule_interval="0 0 * 1 *",
) as dag:

    e1 = EmptyOperator(task_id="pre_dbt")

    e2 = EmptyOperator(task_id="seed_tg")

    e4 = EmptyOperator(task_id="stg_tg")

    e3 = EmptyOperator(task_id="dbt_tg")
    
    e5 = EmptyOperator(task_id="post_dbt")

    e1 >> e2 >> e3 >> e4 >> e5
