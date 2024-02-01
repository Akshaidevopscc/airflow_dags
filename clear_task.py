import json
import requests
import getpass

def export_credential(path):
    domain = input("Domain: ")
    username = input("UserName: ")
    password = getpass.getpass("Password: ")
    credentials = {
        "Username": username,
        "Password": password,
        "Domain": domain
    }
    with open(path, 'w') as file:
        json.dump(credentials, file)

def clear_dag_run(profile, dag_id, dag_run_id):
    cred_path = f"{profile}.json"

    try:
        with open(cred_path) as file:
            file_content = file.read()
            credentials = json.loads(file_content)
    except Exception as e:
        print(f"Error: {e}")
        export_credential(cred_path)
        print("Credentials exported. Rerun the script.")
        exit()

    username = credentials["Username"]
    password = credentials["Password"]
    domain = credentials["Domain"]

    uri = f"https://{domain}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/clear"
    
    headers = {
        "Content-Type": "application/json"
    }
    
    data = {
        "dry_run": False
    }

    response = requests.post(uri, auth=(username, password), headers=headers, json=data)

    if response.status_code == 200:
        print("DAG Run cleared successfully.")
    else:
        print(f"Failed to clear DAG Run. Status Code: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Clear DAG Run")
    parser.add_argument("-profile", required=True, help="Profile")
    parser.add_argument("-Dag", required=True, help="DagId")
    parser.add_argument("-dag_run_id", required=True, help="dag_run_id")
    args = parser.parse_args()

    clear_dag_run(args.profile, args.Dag, args.dag_run_id)
