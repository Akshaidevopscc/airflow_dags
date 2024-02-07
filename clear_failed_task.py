from airflow import DAG
from airflow.models import DagRun
from datetime import datetime, timedelta

def clear_failed_tasks(target_dag_id, target_dag_run_id):
    dagruns = DagRun.find(dag_id=target_dag_id, run_id=target_dag_run_id)
    if dagruns:
        for dagrun in dagruns:
            for ti in dagrun.get_task_instances():
                if ti.state == 'failed':
                    ti.set_state('none')
