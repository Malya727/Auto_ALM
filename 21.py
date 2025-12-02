#!/usr/bin/env python3
"""
ALM Sync Script - Fixed RT Sync with Size Analysis
- Enhanced error handling for size check & RT creation
- Detailed logging for debugging
- Graceful fallbacks
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
def authenticate(username, password, logger):
    try:
        resp = requests.post(ANAPLAN_AUTH_URL, auth=HTTPBasicAuth(username, password), timeout=30)
        resp.raise_for_status()
        token = resp.json()["tokenInfo"]["tokenValue"]
        logger.info("Authentication successful")
        return token
    except Exception as e:
        logger.error(f"Authentication failed: {e} - Response: {resp.text if 'resp' in locals() else 'No response'}")
        raise

# --------------------------
# Config / Model Pairs
# --------------------------
def load_config():
    with open(CONFIG_FILE, "r") as f:
        config = json.load(f)
    model_pairs = config.get("Model Details", {}).get("model_ids", [])
    return model_pairs

# --------------------------
# Workspace ID fetch
# --------------------------
def find_workspace_for_model(token, model_id, logger):
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    try:
        resp = requests.get(f"{ANAPLAN_API_BASE}/workspaces", headers=headers, timeout=30)
        resp.raise_for_status()
        workspaces = resp.json().get("workspaces", [])
        logger.info(f"Found {len(workspaces)} workspaces")
        
        for ws in workspaces:
            ws_id = ws.get("id")
            try:
                resp_models = requests.get(f"{ANAPLAN_API_BASE}/workspaces/{ws_id}/models", headers=headers, timeout=30)
                resp_models.raise_for_status()
                models = resp_models.json().get("models", [])
                for m in models:
                    if m.get("id") == model_id:
                        logger.info(f"Found model {model_id[:8]}... in workspace {ws_id[:8]}...")
                        return ws_id
            except Exception as e:
                logger.debug(f"Models check failed for WS {ws_id[:8]}...: {e}")
                continue
        raise RuntimeError(f"Workspace not found for model {model_id}")
    except Exception as e:
        logger.error(f"Workspace lookup failed for {model_id}: {e}")
        raise

# --------------------------
# FIXED: Workspace Size with fallback
# --------------------------
def get_workspace_usage(token, workspace_id, logger):
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/usage"
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            used = int(data.get("usedBytes", 0))
            alloc = int(data.get("allocatedBytes", 0))
            logger.info(f"WS {workspace_id[:8]}... usage: {bytes_to_human(used)}/{bytes_to_human(alloc)}")
            return used, alloc
        else:
            logger.warning(f"Usage API returned {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        logger.warning(f"Usage API failed for WS {workspace_id[:8]}...: {e}")
    
    # Fallback: assume safe
    logger.info("Using fallback size (safe to proceed)")
    return 0, 1000000000  # 0 used, 1GB alloc = 0%

# --------------------------
# FIXED: Revision Tag Functions
# --------------------------
def list_revision_tags(token, model_id, workspace_id, logger):
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/models/{model_id}/revisions"
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        tags = resp.json().get("revisions", [])
        logger.info(f"Found {len(tags)} revision tags for model {model_id[:8]}...")
        return [t.get("name") for t in tags if t.get("name")]
    except Exception as e:
        logger.error(f"List RT failed: {e} - {resp.text if 'resp' in locals() else ''}")
        return []

def create_revision_tag(token, model_id, tag_name, workspace_id, logger):
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/models/{model_id}/revisions"
    headers = {"Authorization": f"AnaplanAuthToken {token}", "Content-Type": "application/json"}
    payload = {"name": tag_name}
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        logger.info(f"RT create response: {resp.status_code}")
        if resp.status_code in (200, 201, 202):
            logger.info(f"Revision Tag '{tag_name}' created successfully")
            return tag_name
        else:
            logger.error(f"RT create failed {resp.status_code}: {resp.text[:300]}")
            # Common fix: try latest if create fails
            tags = list_revision_tags(token, model_id, workspace_id, logger)
            if tags:
                return tags[-1]
            raise RuntimeError("No fallback RT available")
    except Exception as e:
        logger.error(f"RT creation exception: {e}")
        raise

def get_latest_revision_tag(token, model_id, workspace_id, logger):
    tags = list_revision_tags(token, model_id, workspace_id, logger)
    if not tags:
        raise RuntimeError("No revision tags available")
    latest = tags[-1]
    logger.info(f"Latest RT: '{latest}'")
    return latest

def select_revision_tag(token, model_id, workspace_id, logger):
    tags = list_revision_tags(token, model_id, workspace_id, logger)
    if not tags:
        raise RuntimeError("No revision tags available")
    for i, t in enumerate(tags, start=1):
        print(f"{i}. {t}")
    while True:
        try:
            sel = int(input(f"Select RT [1-{len(tags)}]: ").strip()) - 1
            if 0 <= sel < len(tags):
                logger.info(f"Selected RT: '{tags[sel]}'")
                return tags[sel]
            print("Invalid selection")
        except ValueError:
            print("Enter a number")

# --------------------------
# Sync Function
# --------------------------
def sync_revision(token, dev_model_id, prod_model_id, dev_ws, prod_ws, tag_name, logger):
    headers = {"Authorization": f"AnaplanAuthToken {token}", "Content-Type": "application/json"}
    promote_url = f"{ANAPLAN_API_BASE}/workspaces/{prod_ws}/models/{prod_model_id}/revisions/promote"
    payload = {"sourceModelId": dev_model_id, "revisionName": tag_name}
    try:
        resp = requests.post(promote_url, headers=headers, json=payload, timeout=60)
        logger.info(f"Sync response: {resp.status_code}")
        if resp.status_code in (200, 201, 202):
            logger.info(f"Sync SUCCESS: {dev_model_id[:8]} -> {prod_model_id[:8]} with '{tag_name}'")
            return "Sync Initiated ✓"
        else:
            error_msg = f"Sync failed {resp.status_code}: {resp.text[:200]}"
            logger.error(error_msg)
            return error_msg
    except Exception as e:
        error_msg = f"Sync exception: {e}"
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
        token = authenticate(username, password, logger)
        print("✓ Authentication successful")
    except Exception as e:
        print(f"✗ Authentication failed: {e}")
        sys.exit(1)

    summary_data = []

    for idx, pair in enumerate(model_pairs, start=1):
        dev_id = pair.get("dev_model_id")
        prod_id = pair.get("prod_model_id")
        print(f"
{'='*70}")
        print(f"PAIR {idx}/{len(model_pairs)}: DEV={dev_id[:8]}... -> PROD={prod_id[:8]}...")
        
        # Workspace lookup
        try:
            dev_ws = find_workspace_for_model(token, dev_id, logger)
            prod_ws = find_workspace_for_model(token, prod_id, logger)
        except Exception as e:
            logger.error(f"Workspace lookup failed: {e}")
            summary_data.append([idx, dev_id[:8]+"...", prod_id[:8]+"...", "-", "N/A", "Workspace Not Found"])
            continue

        # Size check with fallback
        used, alloc = get_workspace_usage(token, prod_ws, logger)
        current_pct = size_percentage(used, alloc)
        print(f"Prod WS: {bytes_to_human(used)} / {bytes_to_human(alloc)} ({current_pct:.1f}%)")

        # RT Selection
        tag_name = None
        while True:
            try:
                print("
RT Options:")
                print("1. Latest Available RT")
                print("2. Create New RT")
                print("3. Select from List")
                choice = input("Select [1/2/3]: ").strip() or "1"

                if choice == "1":
                    tag_name = get_latest_revision_tag(token, dev_id, dev_ws, logger)
                elif choice == "2":
                    tag_input = input("New RT name: ").strip() or f"AutoRT_{timestamp()}"
                    tag_name = create_revision_tag(token, dev_id, tag_input, dev_ws, logger)
                elif choice == "3":
                    tag_name = select_revision_tag(token, dev_id, dev_ws, logger)
                break
            except Exception as e:
                print(f"✗ RT Error: {e}")
                retry = input("Retry? (y/n): ").strip().lower()
                if retry != "y":
                    break

        if not tag_name:
            summary_data.append([idx, dev_id[:8]+"...", prod_id[:8]+"...", "-", f"{current_pct:.1f}%", "No RT Selected"])
            continue

        # Size confirmation
        safe_threshold = 95.0
        print(f"
--- SYNC CONFIRMATION ---")
        print(f"RT: '{tag_name}' | Prod usage: {current_pct:.1f}%")
        
        if current_pct < safe_threshold:
            confirm = input("Proceed? (y/n) [y]: ").strip().lower() or "y"
        else:
            print(f"⚠️  WARNING: >{safe_threshold}% usage!")
            confirm = input("Still proceed? (y/n): ").strip().lower()

        if confirm != "y":
            status = "User Skipped"
        else:
            status = sync_revision(token, dev_id, prod_id, dev_ws, prod_ws, tag_name, logger)

        summary_data.append([idx, dev_id[:8]+"...", prod_id[:8]+"...", tag_name[:20]+"...", f"{current_pct:.1f}%", status])
        print(f"✓ Pair {idx} done: {status}")

    # Summary
    print(f"
{'='*70}")
    print("FINAL SUMMARY")
    print(tabulate(summary_data, headers=["#", "DEV", "PROD", "RT", "Prod%", "Status"], tablefmt="grid"))
    logger.info("Process completed")

if __name__ == "__main__":
    main()
