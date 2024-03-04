from datetime import datetime
from typing import List, Optional, Union
from airflow import DAG
from airflow.models.dagrun import DagRun
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone
from airflow.utils.db import provide_session
from sqlalchemy.orm.session import Session
from airflow.models import DagRun
from airflow.models.taskinstance import TaskInstance

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
            from airflow.jobs import BackfillJob
            qry = qry.filter(DR.run_id.notlike(BackfillJob.ID_PREFIX + '%'))
        return qry.order_by(DR.execution_date).all()

def func(**kwargs):
    dag_id = 'env_test'
    dr = MyDagRun()
    results = dr.find(dag_id=dag_id)
    dag_run_states = {}
    for dag_run in results:
        if dag_run.run_id not in dag_run_states:
            dag_run_states[dag_run.run_id] = []
        dag_run_states[dag_run.run_id].append(dag_run.state)
    return dag_run_states

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 11, 1),
}

with DAG(dag_id='trace',
         default_args=default_args,
         schedule=None,
         catchup=False
         ) as dag:

    op = PythonOperator(task_id="task",
                        python_callable=func)
