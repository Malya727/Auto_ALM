#!/usr/bin/env python3
"""
ALM Sync Script - RT Sync with Size Analysis
- Logs all steps, backs up previous logs
- Shows DEV->PROD pairs one by one
- 3 RT options: Latest/Create/Select
- Prod workspace size analysis (<95% threshold)
- User confirmation before each sync
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

def size_percentage(used, alloc):
    return (used / alloc * 100) if alloc > 0 else 0

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
    model_pairs = config.get("Model Details", {}).get("model_ids", [])
    return model_pairs

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
# Revision Tag Functions
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
    if resp.status_code in (201, 202):
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

def select_revision_tag(token, model_id, workspace_id):
    tags = list_revision_tags(token, model_id, workspace_id)
    if not tags:
        raise RuntimeError(f"No Revision Tags available for model {model_id}")
    for i, t in enumerate(tags, start=1):
        print(f"{i}. {t}")
    sel = int(input(f"Select RT [1-{len(tags)}]: ").strip()) - 1
    if 0 <= sel < len(tags):
        return tags[sel]
    raise RuntimeError("Invalid selection")

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
# Sync Function
# --------------------------
def sync_revision(token, dev_model_id, prod_model_id, dev_ws, prod_ws, tag_name, logger):
    headers = {"Authorization": f"AnaplanAuthToken {token}", "Content-Type": "application/json"}
    promote_url = f"{ANAPLAN_API_BASE}/workspaces/{prod_ws}/models/{prod_model_id}/revisions/promote"
    payload = {"sourceModelId": dev_model_id, "revisionName": tag_name}
    resp = requests.post(promote_url, headers=headers, json=payload, timeout=60)
    if resp.status_code in (200, 201, 202):
        logger.info(f"Sync initiated: {dev_model_id} -> {prod_model_id} with RT '{tag_name}'")
        return "Sync Initiated"
    else:
        error_msg = f"Sync failed: {resp.status_code} {resp.text}"
        logger.error(error_msg)
        return error_msg

# --------------------------
# Main
# --------------------------
def main():
    archive_old_logs()
    logger = setup_logger()

    model_pairs = load_config()
    print(f"Loaded {len(model_pairs)} DEV->PROD pairs")

    username = input("Enter Anaplan Username/Email: ").strip()
    password = pwinput.pwinput("Enter Anaplan password: ")
    try:
        token = authenticate(username, password)
        logger.info(f"User '{username}' authenticated successfully")
        print("✓ Authentication successful")
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        print(f"✗ Authentication failed: {e}")
        sys.exit(1)

    summary_data = []

    for idx, pair in enumerate(model_pairs, start=1):
        dev_id = pair.get("dev_model_id")
        prod_id = pair.get("prod_model_id")
        print(f"
{'='*60}")
        print(f"PAIR {idx}/{len(model_pairs)}: DEV={dev_id[:8]}... -> PROD={prod_id[:8]}...")
        
        try:
            dev_ws = find_workspace_for_model(token, dev_id)
            prod_ws = find_workspace_for_model(token, prod_id)
            logger.info(f"Found workspaces: DEV={dev_ws[:8]}..., PROD={prod_ws[:8]}...")
        except Exception as e:
            logger.error(f"Workspace lookup failed for pair {idx}: {e}")
            summary_data.append([idx, dev_id[:8]+"...", prod_id[:8]+"...", "-", "-", "Workspace Not Found"])
            continue

        # Get Prod workspace current usage
        try:
            used, alloc = get_workspace_usage(token, prod_ws)
            current_pct = size_percentage(used, alloc)
            print(f"Prod WS Current: {bytes_to_human(used)} / {bytes_to_human(alloc)} ({current_pct:.1f}%)")
        except Exception as e:
            logger.error(f"Size check failed: {e}")
            used, alloc = 0, 0
            current_pct = 0

        # Revision Tag Options
        tag_name = None
        while True:
            try:
                print("
RT Options:")
                print("1. Sync Latest Available RT")
                print("2. Create New RT and Sync")
                print("3. Select from Available RTs")
                choice = input("Select option [1/2/3]: ").strip() or "1"

                if choice == "1":
                    tag_name = get_latest_revision_tag(token, dev_id, dev_ws)
                    print(f"✓ Latest RT: '{tag_name}'")
                elif choice == "2":
                    tag_input = input("Enter new RT name: ").strip() or f"AutoRT_{timestamp()}"
                    tag_name = create_revision_tag(token, dev_id, tag_input, dev_ws, logger)
                    print(f"✓ Created RT: '{tag_name}'")
                elif choice == "3":
                    tag_name = select_revision_tag(token, dev_id, dev_ws)
                    print(f"✓ Selected RT: '{tag_name}'")
                else:
                    print("Invalid option, using latest RT")
                    tag_name = get_latest_revision_tag(token, dev_id, dev_ws)
                
                break
            except Exception as e:
                print(f"✗ RT selection failed: {e}")
                retry = input("Retry? (y/n): ").strip().lower()
                if retry != "y":
                    summary_data.append([idx, dev_id[:8]+"...", prod_id[:8]+"...", "-", f"{current_pct:.1f}%", "RT Selection Failed"])
                    break

        if not tag_name:
            continue

        # Size Analysis & User Confirmation
        safe_threshold = 95.0
        print(f"
--- Size Analysis ---")
        print(f"Current: {current_pct:.1f}%")
        
        # Note: Exact size increase prediction is complex, using conservative estimate
        proceed = False
        if current_pct < safe_threshold:
            print(f"✓ Current usage < {safe_threshold}% threshold - SAFE TO PROCEED")
            confirm = input("Proceed with sync? (y/n) [y]: ").strip().lower() or "y"
            proceed = confirm == "y"
        else:
            print(f"⚠️  Current usage >= {safe_threshold}% - HIGH RISK!")
            print("Sync may cause workspace quota exceedance.")
            confirm = input("Still proceed with sync? (y/n): ").strip().lower()
            proceed = confirm == "y"

        if not proceed:
            status = "User Skipped"
            summary_data.append([idx, dev_id[:8]+"...", prod_id[:8]+"...", tag_name, f"{current_pct:.1f}%", status])
            continue

        # Execute Sync
        status = sync_revision(token, dev_id, prod_id, dev_ws, prod_ws, tag_name, logger)
        summary_data.append([idx, dev_id[:8]+"...", prod_id[:8]+"...", tag_name, f"{current_pct:.1f}%", status])

        print(f"✓ Pair {idx} completed: {status}")

    # Final Summary Table
    print(f"
{'='*60}")
    print("ALM SYNC SUMMARY")
    print(tabulate(summary_data, headers=["#", "DEV", "PROD", "RT", "Prod %", "Status"], tablefmt="grid"))
    logger.info("Sync process completed")

if __name__ == "__main__":
    main()
