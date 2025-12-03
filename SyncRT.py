import base64
import requests
import time

# ---------------------------
# AUTHENTICATION
# ---------------------------
def authenticate(username, password):
    url = "https://auth.anaplan.com/token/authenticate"
    headers = {
        "Authorization": "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode(),
        "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers)

    if response.status_code not in [200, 201]:
        raise Exception(f"Authentication failed: {response.status_code} - {response.text}")

    token = response.json()['tokenInfo']['tokenValue']
    return token


# ---------------------------
# TRIGGER SYNC (USING PROVIDED RT REVISION ID)
# ---------------------------
def trigger_sync(token, prod_ws, prod_model, rt_revision_id):
    url = f"https://api.anaplan.com/2/0/workspaces/{prod_ws}/models/{prod_model}/tasks/modelSynchronization"

    headers = {
        "Authorization": f"AnaplanAuthToken {token}",
        "Content-Type": "application/json"
    }

    payload = {
        "sourceRevisionId": rt_revision_id
    }

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code not in [200, 201]:
        raise Exception(f"Failed to create sync task: {response.status_code} - {response.text}")

    task_id = response.json()['task']['taskId']
    return task_id


# ---------------------------
# POLL TASK UNTIL COMPLETE
# ---------------------------
def poll_sync(token, prod_ws, prod_model, task_id):
    url = f"https://api.anaplan.com/2/0/workspaces/{prod_ws}/models/{prod_model}/tasks/{task_id}"

    headers = {
        "Authorization": f"AnaplanAuthToken {token}",
        "Content-Type": "application/json"
    }

    while True:
        response = requests.get(url, headers=headers)

        if response.status_code not in [200, 201]:
            raise Exception(f"Error checking task status: {response.status_code} - {response.text}")

        task = response.json()['task']
        status = task['taskState']

        print(f"Sync Status: {status}")

        if status in ("COMPLETE", "FAILED", "CANCELLED"):
            return status

        time.sleep(5)


# ---------------------------
# MAIN FUNCTION
# ---------------------------
def sync_revision_to_prod(username, password, prod_ws, prod_model, rt_revision_id):
    print("Authenticating...")
    token = authenticate(username, password)
    print("✔ Authenticated")

    print("Creating sync task...")
    task_id = trigger_sync(token, prod_ws, prod_model, rt_revision_id)
    print(f"✔ Sync Task Created: {task_id}")

    print("Polling task...")
    status = poll_sync(token, prod_ws, prod_model, task_id)

    if status == "COMPLETE":
        print("✔ Sync Completed Successfully!")
    else:
        print(f"❌ Sync Failed: {status}")
