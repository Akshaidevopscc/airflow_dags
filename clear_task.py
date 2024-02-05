import json
import requests
import getpass
import argparse

def task_clear(profile, username, password, domain, dag_id, dag_run_id, task_ids):
    def clear_task_instances(profile, dag_id, dag_run_id, task_ids, username, password, domain):
        uri = f"https://{domain}/api/v1/dags/{dag_id}/clearTaskInstances"
        headers = {"Content-Type": "application/json"}
        data = {
            "dry_run": False,
            "task_ids": task_ids,
            "dag_run_id": dag_run_id,
            "only_failed": True,
            "only_running": False,
            "include_subdags": True,
            "include_parentdag": True,
            "reset_dag_runs": True,
            "include_upstream": True,
            "include_downstream": True,
            "include_future": False,
            "include_past": False
        }
        response = requests.post(uri, auth=(username, password), headers=headers, json=data)
        if response.status_code == 200:
            print("Task instances cleared successfully.")
        else:
            print(f"Failed to clear task instances. Status Code: {response.status_code}")
            print(response.text)

    clear_task_instances(profile, dag_id, dag_run_id, task_ids, username, password, domain)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clear Task Instances")
    parser.add_argument("-profile", required=True, help="Profile")
    parser.add_argument("-dag_id", required=True, help="DagId")
    parser.add_argument("-dag_run_id", required=True, help="dag_run_id")
    parser.add_argument("-task_ids", nargs='+', required=True, help="List of task IDs")
    args = parser.parse_args()

    task_clear(args.profile, "apitest", "mnbvcxz", "mpmathew-test-poc.03907124.lowtouch.cloud", args.dag_id, args.dag_run_id, args.task_ids)
