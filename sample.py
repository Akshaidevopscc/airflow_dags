from pendulum import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from pathlib import Path

PROJECT_HOME = Variable.get("DBT_ROOT")
DBT_EXECPATH = Variable.get("DBT_EXECPATH")
JS_DOC_PATH = Variable.get("JS_DOC_PATH")

default_args = {
    "retries": 1,
    "owner": "data_engineering",
}

doc_md_DAG = """
* [Data documentation](https:/docs/jaffle_shop/index.html).
* Maintained by [DataEngineering@tcw.com](mailto:DataEngineering@tcw.com).
"""

with DAG(
    dag_id="DAG-Docs",
    start_date=datetime(2024, 2, 9),
    schedule_interval="0 1 * * *",  # 1am PST schedule
    description="A DAG for generating DBT documentation.",
    trigger_rule='all_done', # This ensures the task runs even if the previous task fails
    doc_md=doc_md_DAG,
    default_args=default_args,
    tags=["DOCUMENTATION"],
) as dag:
    with TaskGroup("Generate_Docs") as doc_generate:
        BashOperator(
            task_id="dbt-salesforce-tdc",
            bash_command=f"{DBT_EXECPATH} docs generate --target dev_snowflake --target-path /appz/home/airflow/docs/dbt-salessforce-tdc " +
               "--project-dir /appz/home/airflow/dags/dbt/tcw-dbt-snowflake/dbt-salesforce-tdc " +
               "--profiles-dir /appz/home/airflow/dags/dbt/tcw-dbt-snowflake/dbt-salesforce-tdc"      
        )

        BashOperator(
            task_id="dbt-aladdin",
            bash_command=f"{DBT_EXECPATH} deps --project-dir /appz/home/airflow/dags/dbt/tcw-dbt-snowflake/dbt-aladdin && " +
               f"{DBT_EXECPATH} docs generate --target dev_snowflake --target-path /appz/home/airflow/docs/dbt-aladdin " +
               "--project-dir /appz/home/airflow/dags/dbt/tcw-dbt-snowflake/dbt-aladdin " +
               "--profiles-dir /appz/home/airflow/dags/dbt/tcw-dbt-snowflake/dbt-aladdin"      
        )

        BashOperator(
            task_id="EDI_BACKFILL_TCW_DBT_CORE",
            bash_command=f"{DBT_EXECPATH} deps --project-dir /appz/home/airflow/dags/dbt/tcw-dbt-core/ENTERPRISE_DATA && " +
               f"{DBT_EXECPATH} docs generate --target poc --target-path /appz/home/airflow/docs/tcw-dbt-core/ENTERPRISE_DATA " +
               "--project-dir /appz/home/airflow/dags/dbt/tcw-dbt-core/ENTERPRISE_DATA " +
               "--profiles-dir /appz/home/airflow/dags/dbt/tcw-dbt-core/ENTERPRISE_DATA"      
        )

        BashOperator(
            task_id="Jaffle_Shop",
            bash_command=f"{DBT_EXECPATH} docs generate --target postgres --target-path {JS_DOC_PATH} " +
                f"--project-dir {PROJECT_HOME}/dbt-poc-sample --profiles-dir {PROJECT_HOME}/dbt-poc-sample"
        )

        BashOperator(
            task_id="FidelityFNX",
            bash_command=f"{DBT_EXECPATH} docs generate --target snowflake --target-path /appz/home/airflow/docs/FidelityFNX " +
                "--project-dir /appz/home/airflow/dags/dbt/corporate-services/dbt-core/FidelityFNX " +
                "--profiles-dir /appz/home/airflow/dags/dbt/corporate-services/dbt-core/FidelityFNX"
        )

        BashOperator(
            task_id="ESG",
            bash_command=f"{DBT_EXECPATH} deps --project-dir  /appz/home/airflow/dags/dbt/tcw-dbt-snowflake/dbt-esg && " +
                f"{DBT_EXECPATH} docs generate --target poc_snowflake --target-path /appz/home/airflow/docs/ESG " +
                "--project-dir /appz/home/airflow/dags/dbt/tcw-dbt-snowflake/dbt-esg " + 
                "--profiles-dir /appz/home/airflow/dags/dbt/tcw-dbt-snowflake/dbt-esg"
        )

        BashOperator(
            task_id="ENT",
            bash_command=f"{DBT_EXECPATH} deps --project-dir /appz/home/airflow/dags/dbt/tcw-dbt-snowflake/dbt-enterprise-data && " +
                f"{DBT_EXECPATH} docs generate --target poc_snowflake --target-path /appz/home/airflow/docs/ENT " +
                "--project-dir /appz/home/airflow/dags/dbt/tcw-dbt-snowflake/dbt-enterprise-data " + 
                "--profiles-dir /appz/home/airflow/dags/dbt/tcw-dbt-snowflake/dbt-esg"
        )

        BashOperator(
            task_id="Structured-Products",
            bash_command=f"{DBT_EXECPATH} deps --project-dir /appz/home/airflow/dags/dbt/dbt-structured-products && " +
                f"{DBT_EXECPATH} docs generate --target poc_snowflake " +
                "--target-path /appz/home/airflow/docs/dbt-structured-products " +
                "--project-dir /appz/home/airflow/dags/dbt/dbt-structured-products " +
                "--profiles-dir /appz/home/airflow/dags/dbt/dbt-structured-products"
        )
