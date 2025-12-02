#!/usr/bin/env python3
"""
ALM Sync Script - Full Robust Version
- Works with DEV->PROD pairs from config.json
- Supports 3 RT options (latest / create / select)
- Shows tables using tabulate
- Workspace size estimation in MB/GB
- Logs all steps
"""

import os
import sys
import json
import time
import shutil
import logging
from datetime import datetime
import requests
import pwinput
from requests.auth import HTTPBasicAuth
from tabulate import tabulate

# --------------------------
# Config / Constants
# --------------------------
CONFIG_FILE = "config.json"
LOG_DIR = "Logs"
LOG_BACKUP_DIR = "Log_Backup"
MODEL_HISTORY_DIR = "Model_History"
ANAPLAN_AUTH_URL = "https://auth.anaplan.com/token/authenticate"
ANAPLAN_API_BASE = "https://api.anaplan.com/2/0"

# --------------------------
# Utilities
# --------------------------
def timestamp():
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def bytes_to_human(nbytes):
    gb = 1024**3
    mb = 1024**2
    if nbytes >= gb:
        return f"{nbytes/gb:.2f} GB"
    else:
        return f"{nbytes/mb:.2f} MB"

# --------------------------
# Logging
# --------------------------
def archive_old_logs():
    ensure_dir(LOG_BACKUP_DIR)
    for f in os.listdir("."):
        if f.endswith(".log"):
            shutil.move(f, os.path.join(LOG_BACKUP_DIR, f))

def setup_logger():
    ensure_dir(LOG_DIR)
    logfile = os.path.join(LOG_DIR, f"Auto_ALM_{timestamp()}.log")
    logger = logging.getLogger("ALM")
    logger.setLevel(logging.DEBUG)
    if logger.handlers:
        logger.handlers.clear()
    fh = logging.FileHandler(logfile)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info(f"Log file: {logfile}")
    return logger

# --------------------------
# Authentication
# --------------------------
def authenticate(username, password):
    resp = requests.post(ANAPLAN_AUTH_URL, auth=HTTPBasicAuth(username, password), timeout=30)
    resp.raise_for_status()
    return resp.json()["tokenInfo"]["tokenValue"]

# --------------------------
# Config / Model Pairs
# --------------------------
def load_config():
    with open(CONFIG_FILE, "r") as f:
        config = json.load(f)
    export_name = config.get("Model Details", {}).get("export_action_name", "")
    model_pairs = config.get("Model Details", {}).get("model_ids", [])
    return export_name, model_pairs

# --------------------------
# Workspace ID fetch by scanning all workspaces
# --------------------------
def find_workspace_for_model(token, model_id):
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    resp = requests.get(f"{ANAPLAN_API_BASE}/workspaces", headers=headers, timeout=30)
    resp.raise_for_status()
    workspaces = resp.json().get("workspaces", [])
    for ws in workspaces:
        ws_id = ws.get("id")
        resp_models = requests.get(f"{ANAPLAN_API_BASE}/workspaces/{ws_id}/models", headers=headers, timeout=30)
        resp_models.raise_for_status()
        models = resp_models.json().get("models", [])
        for m in models:
            if m.get("id") == model_id:
                return ws_id
    raise RuntimeError(f"Workspace not found for model {model_id}")

# --------------------------
# Model History Export
# --------------------------
def find_export_id(token, model_id, export_name):
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    url = f"{ANAPLAN_API_BASE}/models/{model_id}/exports"
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    exports = resp.json().get("exports", [])
    for e in exports:
        if e.get("name") == export_name:
            return e.get("id")
    raise RuntimeError(f"Export '{export_name}' not found for model {model_id}")

def run_export(token, workspace_id, model_id, export_id, out_dir, logger):
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/models/{model_id}/exports/{export_id}/tasks"
    payload = {"localeName": "en_US"}
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    if r.status_code != 200:
        logger.error(f"Failed to start export: {r.status_code} {r.text}")
        raise RuntimeError(f"Export failed with status {r.status_code}")
    task_data = r.json()
    task_id = task_data.get("task", {}).get("id")
    if task_id:
        task_url = f"{url}/{task_id}"
        for _ in range(60):
            time.sleep(1)
            t_resp = requests.get(task_url, headers=headers, timeout=30)
            t_resp.raise_for_status()
            status = t_resp.json().get("task", {}).get("status") or t_resp.json().get("status")
            if status and str(status).lower() in ("completed","success"):
                logger.info("Export task completed.")
                break
        else:
            raise RuntimeError("Export task did not complete in expected time")
    ensure_dir(out_dir)
    download_url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/models/{model_id}/exports/{export_id}/result"
    dl = requests.get(download_url, headers=headers, stream=True, timeout=30)
    dl.raise_for_status()
    fname = f"{model_id}_{timestamp()}.zip"
    fpath = os.path.join(out_dir, fname)
    with open(fpath, "wb") as fh:
        for chunk in dl.iter_content(8192):
            if chunk: fh.write(chunk)
    logger.info(f"Downloaded model history: {fpath}")
    return fpath

# --------------------------
# Revision Tag
# --------------------------
def list_revision_tags(token, model_id, workspace_id):
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/models/{model_id}/revisions"
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    tags = resp.json().get("revisions", [])
    return [t.get("name") for t in tags]

def create_revision_tag(token, model_id, tag_name, workspace_id, logger):
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/models/{model_id}/revisions"
    headers = {"Authorization": f"AnaplanAuthToken {token}", "Content-Type": "application/json"}
    payload = {"name": tag_name}
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    if resp.status_code in (201,202):
        logger.info(f"Revision Tag '{tag_name}' created on model {model_id}")
        return tag_name
    else:
        logger.error(f"Failed to create Revision Tag '{tag_name}': {resp.status_code} {resp.text}")
        raise RuntimeError(f"Revision Tag creation failed: {resp.status_code}")

def get_latest_revision_tag(token, model_id, workspace_id):
    tags = list_revision_tags(token, model_id, workspace_id)
    if not tags:
        raise RuntimeError(f"No Revision Tags available for model {model_id}")
    return tags[-1]

# --------------------------
# Workspace Size
# --------------------------
def get_workspace_usage(token, workspace_id):
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/usage"
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    used = data.get("usedBytes") or 0
    alloc = data.get("allocatedBytes") or 0
    return int(used), int(alloc)

# --------------------------
# Main
# --------------------------
def main():
    archive_old_logs()
    logger = setup_logger()
    ensure_dir(MODEL_HISTORY_DIR)

    export_name, model_pairs = load_config()
    print(f"Export action: {export_name}")

    username = input("Enter Anaplan Username/Email: ").strip()
    password = pwinput.pwinput("Enter Anaplan password: ")
    try:
        token = authenticate(username, password)
        logger.info(f"User '{username}' logged in successfully.")
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        sys.exit(1)

    summary_data = []

    for idx, pair in enumerate(model_pairs, start=1):
        dev_id = pair.get("dev_model_id")
        prod_id = pair.get("prod_model_id")

        try:
            dev_ws = find_workspace_for_model(token, dev_id)
            prod_ws = find_workspace_for_model(token, prod_id)
        except Exception as e:
            logger.error(f"Workspace fetch failed: {e}")
            summary_data.append([idx, dev_id, prod_id, "-", "-", "-", "Workspace Not Found"])
            continue

        # Download Model History
        try:
            export_id = find_export_id(token, dev_id, export_name)
            rev_file = run_export(token, dev_ws, dev_id, export_id, MODEL_HISTORY_DIR, logger)
        except Exception as e:
            logger.error(f"Export failed: {e}")
            summary_data.append([idx, dev_id, prod_id, "-", "-", "-", "Export Failed"])
            continue

        # Revision Tag selection
        while True:
            try:
                print("\nRevision Tag Options: 1-Latest 2-Create 3-Select")
                choice = input("Select option [1/2/3]: ").strip() or "1"

                if choice == "1":
                    tag_name = get_latest_revision_tag(token, dev_id, dev_ws)
                elif choice == "2":
                    tag_name_input = input("Enter new RT name: ").strip() or f"AutoTag_{timestamp()}"
                    tag_name = create_revision_tag(token, dev_id, tag_name_input, dev_ws, logger)
                elif choice == "3":
                    tags = list_revision_tags(token, dev_id, dev_ws)
                    for i, t in enumerate(tags, start=1):
                        print(f"{i}. {t}")
                    sel = int(input(f"Select RT [1-{len(tags)}]: ").strip()) - 1
                    tag_name = tags[sel]
                else:
                    tag_name = get_latest_revision_tag(token, dev_id, dev_ws)
                break
            except Exception as e:
                print(f"RT selection failed: {e}")
                retry = input("Retry? (y/n): ").strip().lower()
                if retry != "y":
                    tag_name = None
                    break

        if not tag_name:
            summary_data.append([idx, dev_id, prod_id, "-", "-", "-", "RT Selection Failed"])
            continue

        # Workspace size
        used, alloc = get_workspace_usage(token, prod_ws)
        rev_size = os.path.getsize(rev_file) if rev_file else 0
        after = used + rev_size
        pct = after / alloc if alloc else 0
        print(f"Prod workspace: {bytes_to_human(used)}/{bytes_to_human(alloc)}, After sync: {bytes_to_human(after)} ({pct:.2%})")

        ans = input("Sync this pair? (y/n) [y]: ").strip().lower() or "y"
        if ans != "y":
            summary_data.append([idx, dev_id, prod_id, tag_name, bytes_to_human(used), bytes_to_human(after), "Skipped"])
            continue

        # Sync
        headers = {"Authorization": f"AnaplanAuthToken {token}", "Content-Type": "application/json"}
        promote_url = f"{ANAPLAN_API_BASE}/workspaces/{prod_ws}/models/{prod_id}/revisions/promote"
        payload = {"sourceModelId": dev_id, "revisionName": tag_name}
        try:
            resp = requests.post(promote_url, headers=headers, json=payload, timeout=60)
            if resp.status_code in (200,201,202):
                status = "Sync Initiated"
            else:
                status = f"Sync Failed {resp.status_code}"
        except Exception as e:
            status = f"Sync Failed {e}"

        summary_data.append([idx, dev_id, prod_id, tag_name, bytes_to_human(used), bytes_to_human(after), status])

    # Show summary table
    print("\n=== ALM Summary ===")
    print(tabulate(summary_data, headers=["#", "DEV Model", "PROD Model", "RT Selected", "Size Before", "Size After", "Status"], tablefmt="grid"))

if __name__=="__main__":
    main()
