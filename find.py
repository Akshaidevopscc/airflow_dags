from datetime import datetime
from typing import List, Optional, Union
from airflow import DAG
from airflow.models.dagrun import DagRun
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone
from airflow.utils.db import provide_session
from sqlalchemy.orm.session import Session


class MyDagRun(DagRun):

    @staticmethod
    @provide_session
    def find(
        dag_id: Optional[Union[str, List[str]]] = None,
        run_id: Optional[str] = None,
        execution_date: Optional[datetime] = None,
        state: Optional[str] = None,
        external_trigger: Optional[bool] = None,
        no_backfills: bool = False,
        session: Session = None,
        execution_start_date: Optional[datetime] = None,
        execution_end_date: Optional[datetime] = None,
    ) -> List["DagRun"]:

        DR = MyDagRun

        qry = session.query(DR)
        dag_ids = [dag_id] if isinstance(dag_id, str) else dag_id
        if dag_ids:
            qry = qry.filter(DR.dag_id.in_(dag_ids))
        if run_id:
            qry = qry.filter(DR.run_id == run_id)
        if execution_date:
            if isinstance(execution_date, list):
                qry = qry.filter(DR.execution_date.in_(execution_date))
            else:
                qry = qry.filter(DR.execution_date == execution_date)
        if execution_start_date and execution_end_date:
            qry = qry.filter(DR.execution_date.between(execution_start_date, execution_end_date))
        elif execution_start_date:
            qry = qry.filter(DR.execution_date >= execution_start_date)
        elif execution_end_date:
            qry = qry.filter(DR.execution_date <= execution_end_date)
        if state:
            qry = qry.filter(DR.state == state)
        if external_trigger is not None:
            qry = qry.filter(DR.external_trigger == external_trigger)
        if no_backfills:
            # in order to prevent a circular dependency
            from airflow.jobs import BackfillJob
            qry = qry.filter(DR.run_id.notlike(BackfillJob.ID_PREFIX + '%'))

        return qry.order_by(DR.execution_date).all()


def func(**kwargs):
    dag_id = 'airflow_dags_akshai'
    dr = MyDagRun()
    results = dr.find(dag_id=dag_id)

    dag_run_ids = [dag_run.run_id for dag_run in results]
    return dag_run_ids


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 11, 1),
}

with DAG(dag_id='test',
         default_args=default_args,
         schedule_interval=None,
         catchup=True
         ) as dag:

    op = PythonOperator(task_id="task",
                        python_callable=func

################################################################################
