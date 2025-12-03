import requests
import time
import base64


# -------------------------------------------------------
# 1. AUTHENTICATION
# -------------------------------------------------------
def authenticate(username, password):
    url = "https://auth.anaplan.com/token/authenticate"

    auth_string = f"{username}:{password}"
    auth_b64 = base64.b64encode(auth_string.encode("utf-8")).decode("utf-8")

    headers = {
        "Authorization": f"Basic {auth_b64}",
        "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers)

    if response.status_code not in (200, 201):
        raise Exception(f"Authentication Failed: {response.status_code} - {response.text}")

    token = response.json()["tokenInfo"]["tokenValue"]
    return token


# -------------------------------------------------------
# 2. GET REVISION ID BY NAME FROM RT MODEL
# -------------------------------------------------------
def get_revision_id_from_name(token, ws_id, model_id, revision_name):
    url = f"https://api.anaplan.com/2/0/workspaces/{ws_id}/models/{model_id}/revisions"

    headers = {
        "Authorization": f"AnaplanAuthToken {token}",
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)

    if response.status_code not in (200, 201):
        raise Exception(f"Failed to fetch RT revisions: {response.status_code} - {response.text}")

    revisions = response.json().get("revisions", [])

    for rev in revisions:
        if rev["name"].strip().lower() == revision_name.strip().lower():
            return rev["id"]

    raise Exception(f"Revision Tag '{revision_name}' NOT FOUND in RT model")


# -------------------------------------------------------
# 3. TRIGGER SYNC OF STRUCTURAL CHANGES
# -------------------------------------------------------
def trigger_sync(token, prod_ws, prod_model, source_revision_id):
    url = f"https://api.anaplan.com/2/0/workspaces/{prod_ws}/models/{prod_model}/tasks/modelSynchronization"

    payload = {
        "sourceRevisionId": source_revision_id
    }

    headers = {
        "Authorization": f"AnaplanAuthToken {token}",
        "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code not in (200, 201):
        raise Exception(f"Failed to trigger sync: {response.status_code} - {response.text}")

    task_id = response.json()["task"]["taskId"]
    return task_id


# -------------------------------------------------------
# 4. POLL SYNC STATUS UNTIL FINISHED
# -------------------------------------------------------
def poll_sync(token, prod_ws, prod_model, task_id):
    url = f"https://api.anaplan.com/2/0/workspaces/{prod_ws}/models/{prod_model}/tasks/{task_id}"

    headers = {
        "Authorization": f"AnaplanAuthToken {token}",
        "Content-Type": "application/json"
    }

    while True:
        response = requests.get(url, headers=headers)

        if response.status_code not in (200, 201):
            raise Exception(f"Failed to poll sync status: {response.status_code}")

        task_info = response.json()["task"]
        status = task_info["taskState"]

        print(f"Sync Status: {status}")

        if status in ("COMPLETE", "FAILED", "CANCELLED"):
            return status

        time.sleep(5)  # Wait 5 sec before next poll


# -------------------------------------------------------
# 5. MASTER FUNCTION
# -------------------------------------------------------
def sync_revision_by_name(
        username, password,
        rt_ws, rt_model,
        prod_ws, prod_model,
        rt_revision_name):

    print("🔐 Authenticating...")
    token = authenticate(username, password)
    print("✔ Authenticated")

    print(f"🔎 Finding Revision Tag ID for: {rt_revision_name}")
    revision_id = get_revision_id_from_name(token, rt_ws, rt_model, rt_revision_name)
    print(f"✔ Found Revision ID: {revision_id}")

    print("🚀 Triggering Sync Task...")
    task_id = trigger_sync(token, prod_ws, pr_
