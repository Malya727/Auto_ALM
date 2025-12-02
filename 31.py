import json
import requests
from requests.auth import HTTPBasicAuth
from prettytable import PrettyTable
import os
import shutil
import datetime
import getpass
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

CONFIG_PATH = 'config.json'
INPUT_HISTORY_PATH = 'input_history.json'
BASE_URL = 'https://api.anaplan.com/2/0'
LOG_DIR = "logs"
BACKUP_DIR = os.path.join(LOG_DIR, "backup")

lock = threading.Lock()  # To synchronize writes to input history and logs

def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(BACKUP_DIR, exist_ok=True)

    # Move existing logs to backup folder (except backup folder itself)
    for filename in os.listdir(LOG_DIR):
        file_path = os.path.join(LOG_DIR, filename)
        if os.path.isfile(file_path) and not filename.startswith("backup"):
            shutil.move(file_path, BACKUP_DIR)

    now_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(LOG_DIR, f"alm_log_{now_str}.txt")
    return log_file

def log_write(log_file, message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with lock:
        with open(log_file, 'a') as f:
            f.write(f"[{timestamp}] {message}\n")
        print(message)

def load_config():
    with open(CONFIG_PATH, 'r') as f:
        return json.load(f)

def load_input_history():
    if os.path.exists(INPUT_HISTORY_PATH):
        with open(INPUT_HISTORY_PATH, 'r') as f:
            return json.load(f)
    else:
        return {}

def save_input_history(history):
    with lock:
        with open(INPUT_HISTORY_PATH, 'w') as f:
            json.dump(history, f, indent=2)

def auth_from_input(username, password):
    return HTTPBasicAuth(username, password)

def find_workspace_by_model(auth, model_id):
    r = requests.get(f'{BASE_URL}/workspaces', auth=auth)
    r.raise_for_status()
    workspaces = r.json()['workspaces']
    for ws in workspaces:
        r2 = requests.get(f"{BASE_URL}/workspaces/{ws['id']}/models", auth=auth)
        r2.raise_for_status()
        models = r2.json()['models']
        if any(m['id'].upper() == model_id.upper() for m in models):
            return ws['id']
    return None

def list_revisions(auth, model_id):
    workspace_id = find_workspace_by_model(auth, model_id)
    if workspace_id is None:
        raise RuntimeError(f"Model ID {model_id} not found in any workspace")
    url = f"{BASE_URL}/workspaces/{workspace_id}/models/{model_id}/revisions"
    r = requests.get(url, auth=auth)
    r.raise_for_status()
    return r.json().get('revisions', [])

def promote_revision(auth, dev_model_id, prod_model_id, revision_id):
    dev_workspace_id = find_workspace_by_model(auth, dev_model_id)
    prod_workspace_id = find_workspace_by_model(auth, prod_model_id)
    if not dev_workspace_id or not prod_workspace_id:
        raise RuntimeError("Workspace not found for Dev or Prod model.")

    url = f"{BASE_URL}/workspaces/{dev_workspace_id}/models/{dev_model_id}/promote"
    payload = {
        "targetWorkspaceId": prod_workspace_id,
        "targetModelId": prod_model_id,
        "revisionId": revision_id
    }
    r = requests.post(url, auth=auth, json=payload)
    return r

def print_revisions_table(revisions):
    table = PrettyTable()
    table.field_names = ["Index", "Revision ID", "Name", "Created By", "Created At"]
    for i, rev in enumerate(revisions, start=1):
        created_by = rev.get('createdBy', {}).get('name', 'N/A')
        table.add_row([i, rev.get('id'), rev.get('name'), created_by, rev.get('createdAt')])
    print(table)

def select_revision(revisions, saved_choice=None):
    if saved_choice is not None and 1 <= saved_choice <= len(revisions):
        return revisions[saved_choice-1]
    rev_choice = input(f"Select a revision to promote (1-{len(revisions)}), or 0 to skip: ").strip()
    while not (rev_choice.isdigit() and 0 <= int(rev_choice) <= len(revisions)):
        rev_choice = input(f"Invalid. Select a revision to promote (1-{len(revisions)}), or 0 to skip: ").strip()
    if rev_choice == '0':
        return None
    return revisions[int(rev_choice)-1], int(rev_choice)

def process_single_model(auth, log_file, model, saved_inputs):
    dev_model_id = model['dev_model_id']
    prod_model_id = model['prod_model_id']
    model_key = f"{dev_model_id}_{prod_model_id}"

    log_write(log_file, f"\n=== Processing Model Pair ===")
    log_write(log_file, f"Dev Model ID : {dev_model_id}")
    log_write(log_file, f"Prod Model ID: {prod_model_id}")

    try:
        revisions = list_revisions(auth, dev_model_id)
    except Exception as e:
        log_write(log_file, f"Error fetching revisions: {e}")
        return None

    if not revisions:
        log_write(log_file, "No revisions found for this dev model.")
        return None

    # Show revisions table
    print_revisions_table(revisions)

    prev_choice = saved_inputs.get(model_key, {}).get('selected_revision_index')
    if prev_choice:
        # Use previously selected revision index (if valid)
        selected_rev = None
        if 1 <= prev_choice <= len(revisions):
            selected_rev = revisions[prev_choice-1]
        else:
            selected_rev = None
    else:
        # Ask user
        selected = select_revision(revisions)
        if selected is None:
            log_write(log_file, "Skipped promotion for this model.")
            return None
        selected_rev = selected[0] if isinstance(selected, tuple) else selected
        prev_choice = selected[1] if isinstance(selected, tuple) else None

    if not selected_rev:
        log_write(log_file, "No revision selected, skipping.")
        return None

    log_write(log_file, f"Selected Revision to Promote: {selected_rev['name']} (ID: {selected_rev['id']})")

    # Save input choice to history
    with lock:
        saved_inputs[model_key] = {
            'selected_revision_id': selected_rev['id'],
            'selected_revision_index': prev_choice
        }
        save_input_history(saved_inputs)

    try:
        resp = promote_revision(auth, dev_model_id, prod_model_id, selected_rev['id'])
        if resp.status_code == 200:
            log_write(log_file, f"Promotion succeeded for model pair {model_key}.")
        else:
            log_write(log_file, f"Promotion failed ({resp.status_code}) for model pair {model_key}: {resp.text}")
    except Exception as e:
        log_write(log_file, f"Promotion exception for model pair {model_key}: {e}")

def main():
    log_file = setup_logging()
    config = load_config()
    saved_inputs = load_input_history()

    print("\n--- Anaplan ALM Automation (Parallel Model Processing with Input History) ---\n")
    username = input("Enter Anaplan username (email): ").strip()
    password = getpass.getpass("Enter Anaplan password: ").strip()
    auth = auth_from_input(username, password)

    models = config["Model Details"].get("model_ids", [])
    if not models:
        print("No model pairs found in configuration.")
        return

    log_write(log_file, "Starting ALM Automation with models from config")

    # Parallel processing all models
    with ThreadPoolExecutor(max_workers=len(models)) as executor:
        futures = {executor.submit(process_single_model, auth, log_file, model, saved_inputs): model for model in models}
        for future in as_completed(futures):
            model = futures[future]
            try:
                future.result()
            except Exception as exc:
                log_write(log_file, f"Model {model} generated an exception: {exc}")

    log_write(log_file, "\nAll models processed. Exiting.")

if __name__ == "__main__":
    main()
