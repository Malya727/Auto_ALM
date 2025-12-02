#!/usr/bin/env python3
"""
auto_alm_classic.py

Final Classic-ALM automation script using workspace IDs from config.json.
- Reads config.json (Model Details -> model_ids)
- Authenticates via Basic Auth -> AnaplanAuthToken
- Lists/creates Revision Tags in DEV
- Promotes Revision Tag to PROD using Revision API, with fallback to classic Actions->Tasks promote
- Logs to Logs/AUTO_ALM_<ts>.log, archives old logs into Log_Backup/
- Shows tables for pairs and summary
"""

import os
import sys
import json
import time
import shutil
import logging
from datetime import datetime
from getpass import getpass

import requests
from requests.auth import HTTPBasicAuth
from tabulate import tabulate

# optional nicer password prompt
try:
    import pwinput
except Exception:
    pwinput = None

# -------------------------
# Config / constants
# -------------------------
CONFIG_FILE = "config.json"
LOG_DIR = "Logs"
LOG_BACKUP_DIR = "Log_Backup"
ANAPLAN_AUTH_URL = "https://auth.anaplan.com/token/authenticate"
ANAPLAN_API_BASE = "https://api.anaplan.com/2/0"

REQUEST_TIMEOUT = 30
POLL_DELAY = 1
RT_RETRY = 2
PROMOTE_RETRY = 2

# -------------------------
# Helpers
# -------------------------
def ts():
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def ensure_dir(p):
    if not os.path.exists(p):
        os.makedirs(p, exist_ok=True)

def bytes_to_human(nbytes):
    try:
        n = int(nbytes)
    except Exception:
        return "0 MB"
    GB = 1024**3
    MB = 1024**2
    if n >= GB:
        return f"{n/GB:.2f} GB"
    else:
        return f"{n/MB:.2f} MB"

# -------------------------
# Logging
# -------------------------
def archive_old_logs(logger=None):
    ensure_dir(LOG_BACKUP_DIR)
    files = [f for f in os.listdir(".") if f.endswith(".log")]
    for f in files:
        try:
            shutil.move(f, os.path.join(LOG_BACKUP_DIR, f))
            if logger:
                logger.debug(f"Archived {f} -> {LOG_BACKUP_DIR}")
        except Exception as e:
            if logger:
                logger.warning(f"Could not archive {f}: {e}")

def setup_logger():
    ensure_dir(LOG_DIR)
    logfile = os.path.join(LOG_DIR, f"AUTO_ALM_{ts()}.log")
    logger = logging.getLogger("AUTO_ALM")
    logger.setLevel(logging.DEBUG)
    if logger.handlers:
        logger.handlers.clear()

    fh = logging.FileHandler(logfile, mode="w", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(message)s"))

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info(f"Log file: {logfile}")
    return logger

# -------------------------
# Authentication
# -------------------------
def authenticate(username, password, logger):
    logger.info("Authenticating to Anaplan...")
    try:
        r = requests.post(ANAPLAN_AUTH_URL, auth=HTTPBasicAuth(username, password), timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        logger.error(f"Auth request failed: {e}")
        raise
    try:
        token = r.json().get("tokenInfo", {}).get("tokenValue")
        if not token:
            logger.error(f"Auth response missing token: {r.text}")
            raise RuntimeError("Token missing in auth response")
        logger.info("Authentication successful.")
        return token
    except Exception as e:
        logger.error(f"Failed parsing auth response: {e}")
        raise

# -------------------------
# Config loader
# -------------------------
def load_config(logger):
    if not os.path.exists(CONFIG_FILE):
        logger.error(f"Config file not found: {CONFIG_FILE}")
        raise FileNotFoundError(CONFIG_FILE)
    with open(CONFIG_FILE, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)
    model_ids = cfg.get("Model Details", {}).get("model_ids", [])
    if not model_ids:
        logger.error("No model pairs found under 'Model Details'->'model_ids' in config.json")
    return model_ids

# -------------------------
# Model & workspace helpers (workspace IDs provided)
# -------------------------
def get_model_name(token, workspace_id, model_id, logger):
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/models/{model_id}"
    logger.debug(f"GET {url}")
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json().get("name") or "<unknown>"

def get_workspace_usage(token, workspace_id, logger):
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/usage"
    logger.debug(f"GET {url}")
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    j = r.json()
    used = j.get("usedBytes") or j.get("used") or 0
    alloc = j.get("allocatedBytes") or j.get("allocated") or 0
    return int(used), int(alloc)

# -------------------------
# Revision Tag functions (Revision API)
# -------------------------
def list_revision_tags(token, workspace_id, model_id, logger):
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/models/{model_id}/revisions"
    logger.debug(f"Listing RTs: GET {url}")
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json().get("revisions", [])

def create_revision_tag(token, workspace_id, model_id, tag_name, logger):
    headers = {"Authorization": f"AnaplanAuthToken {token}", "Content-Type": "application/json"}
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/models/{model_id}/revisions"
    payload = {"name": tag_name}
    logger.info(f"Creating RT '{tag_name}' on DEV model {model_id} (ws {workspace_id})")
    r = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
    if r.status_code in (201, 202):
        try:
            return r.json()
        except Exception:
            return {"name": tag_name}
    else:
        logger.error(f"Create RT failed: {r.status_code} {r.text}")
        r.raise_for_status()

# -------------------------
# Classic Actions-based helpers (fallback)
# -------------------------
def list_model_actions(token, workspace_id, model_id, logger):
    """
    GET /workspaces/{ws}/models/{model}/actions
    Returns list of actions (dicts)
    """
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/models/{model_id}/actions"
    logger.debug(f"Listing actions: GET {url}")
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json().get("actions", [])

def start_action_task(token, workspace_id, model_id, action_id, payload, logger):
    """
    POST /workspaces/{ws}/models/{model}/actions/{actionId}/tasks
    """
    headers = {"Authorization": f"AnaplanAuthToken {token}", "Content-Type": "application/json"}
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/models/{model_id}/actions/{action_id}/tasks"
    logger.debug(f"Starting action task: POST {url} payload={payload}")
    r = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT*2)
    # action tasks often return 200/201/202 with a task id structure
    if r.status_code in (200,201,202):
        try:
            return r.json()
        except Exception:
            return {"status_code": r.status_code}
    else:
        logger.error(f"Start action task failed: {r.status_code} {r.text}")
        r.raise_for_status()

# -------------------------
# Promote logic - try revision-promote then fallback to actions
# -------------------------
def promote_revision_classic(token, dev_model_id, dev_workspace_id, prod_model_id, prod_workspace_id, revision_name, logger):
    """
    Primary attempt: POST /workspaces/{prodWs}/models/{prodModel}/revisions/promote
      payload: {"sourceModelId": devModelId, "revisionName": "<name>"}
    If that fails (405/404/other), fallback to scanning actions on prod model for an ALM-like action,
    then POST tasks to that action with a payload that includes sourceModelId + revisionName if supported.
    """
    headers = {"Authorization": f"AnaplanAuthToken {token}", "Content-Type": "application/json"}
    promote_url = f"{ANAPLAN_API_BASE}/workspaces/{prod_workspace_id}/models/{prod_model_id}/revisions/promote"
    payload = {"sourceModelId": dev_model_id, "revisionName": revision_name}
    logger.info(f"Attempting revision-promote via {promote_url} payload={payload}")
    try:
        r = requests.post(promote_url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT*2)
        if r.status_code in (200,201,202):
            try:
                logger.info("Revision promote accepted by API.")
                return {"method": "revision_promote", "response": r.json() if r.text else {"status_code": r.status_code}}
            except Exception:
                return {"method": "revision_promote", "response": {"status_code": r.status_code}}
        else:
            logger.warning(f"Revision-promote returned {r.status_code}: {r.text[:1000]}")
            # proceed to fallback
    except requests.HTTPError as he:
        logger.warning(f"Revision-promote HTTPError: {he}")
    except Exception as e:
        logger.warning(f"Revision-promote failed: {e}")

    # Fallback: try actions-based promote on PROD model
    logger.info("Falling back to actions-based promote (classic). Scanning actions on PROD model...")
    try:
        actions = list_model_actions(token, prod_workspace_id, prod_model_id, logger)
    except Exception as e:
        logger.error(f"Failed to list actions on prod model: {e}")
        raise RuntimeError(f"Promote failed: cannot list actions on prod model: {e}")

    # Heuristic: find an action whose name contains 'alm' or 'promote' or 'revision'
    candidate = None
    for act in actions:
        name = (act.get("name") or "").lower()
        if "alm" in name or "promote" in name or "revision" in name:
            candidate = act
            break
    if not candidate and actions:
        # fallback to first action
        candidate = actions[0]

    if not candidate:
        raise RuntimeError("No actions available on PROD model to perform ALM promote")

    action_id = candidate.get("id")
    action_name = candidate.get("name")
    logger.info(f"Using action id={action_id} name='{action_name}' to attempt promote via tasks")

    # Build payload: many tenants accept {"sourceModelId":..., "revisionName": "..."}
    action_payload = {"sourceModelId": dev_model_id, "revisionName": revision_name}
    try:
        task_result = start_action_task(token, prod_workspace_id, prod_model_id, action_id, action_payload, logger)
        logger.info("Started action task for promote via actions API.")
        return {"method": "actions_task", "action_id": action_id, "response": task_result}
    except Exception as e:
        logger.error(f"Action-task promote attempt failed: {e}")
        raise RuntimeError(f"Promote failed via both revision-promote and actions-based route: {e}")

# -------------------------
# Interactive helpers
# -------------------------
def prompt_for_rt_option(token, dev_ws, dev_model, logger):
    """
    Offer the 3 options and return tuple (action, revision_name_or_None)
    action in {'create','latest','select','skip'}
    """
    while True:
        print("\nChoose Revision Tag option for DEV model", dev_model)
        print("  1) Create a new Revision Tag in DEV (immediately)")
        print("  2) Use latest available Revision Tag in DEV")
        print("  3) List available Revision Tags in DEV and select one")
        print("  4) Skip this pair")
        choice = input("Select [1/2/3/4]: ").strip()
        if choice not in ("1","2","3","4"):
            print("Enter 1,2,3 or 4.")
            continue

        if choice == "4":
            return "skip", None

        if choice == "1":
            name = input("Enter new RT name (leave blank to auto-generate): ").strip()
            if not name:
                name = f"AUTO_RT_{ts()}"
            return "create", name

        # For choices 2 & 3 we must list revisions (with retry)
        attempts = 0
        revisions = None
        while attempts <= RT_RETRY:
            try:
                revisions = list_revision_tags(token, dev_ws, dev_model, logger)
                break
            except Exception as e:
                attempts += 1
                logger.warning(f"Failed to list RTs (attempt {attempts}/{RT_RETRY}): {e}")
                time.sleep(POLL_DELAY)
        if revisions is None:
            print("Could not list Revision Tags. Choose again or skip.")
            cont = input("Retry choosing option? (y/n) [y]: ").strip().lower() or "y"
            if cont != "y":
                return "skip", None
            else:
                continue

        if choice == "2":
            if not revisions:
                print("No RTs found on DEV model. Choose create or skip.")
                continue
            latest = revisions[-1].get("name")
            return "latest", latest

        if choice == "3":
            if not revisions:
                print("No RTs found on DEV model.")
                c = input("Create new RT now? (y/n) [y]: ").strip().lower() or "y"
                if c == "y":
                    name = input("Enter new RT name (leave blank to auto-generate): ").strip()
                    if not name:
                        name = f"AUTO_RT_{ts()}"
                    return "create", name
                else:
                    return "skip", None
            names = [r.get("name") for r in revisions]
            print("\nAvailable RTs:")
            for i, n in enumerate(names, start=1):
                print(f"  {i}) {n}")
            while True:
                sel = input(f"Select RT [1-{len(names)}] or 'c' to cancel: ").strip().lower()
                if sel == "c":
                    break
                if not sel.isdigit():
                    print("Enter a number or 'c'.")
                    continue
                idx = int(sel) - 1
                if idx < 0 or idx >= len(names):
                    print("Out of range.")
                    continue
                return "select", names[idx]
            # canceled the select menu, go back to top

# -------------------------
# Main flow
# -------------------------
def main():
    # 1) Archive old logs
    temp_logger = logging.getLogger("TEMP")
    temp_logger.addHandler(logging.StreamHandler(sys.stdout))
    archive_old_logs(temp_logger)

    # 2) Setup logger
    logger = setup_logger()

    # 3) Load config
    try:
        model_pairs = load_config(logger)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)
    if not model_pairs:
        logger.error("No model pairs; exiting.")
        sys.exit(1)

    # 4) Auth
    username = input("Anaplan username/email: ").strip()
    if pwinput:
        password = pwinput.pwinput("Anaplan password: ")
    else:
        password = getpass("Anaplan password: ")
    try:
        token = authenticate(username, password, logger)
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        sys.exit(1)

    # 5) Enrich pairs (model names, workspace usage) using provided workspace IDs
    enriched = []
    logger.info("Fetching model names and workspace usage for pairs...")
    for idx, p in enumerate(model_pairs, start=1):
        dev_ws = p.get("dev_workspace_id")
        dev_model = p.get("dev_model_id")
        prod_ws = p.get("prod_workspace_id")
        prod_model = p.get("prod_model_id")
        entry = {
            "index": idx,
            "dev_workspace_id": dev_ws,
            "dev_model_id": dev_model,
            "prod_workspace_id": prod_ws,
            "prod_model_id": prod_model
        }
        try:
            dev_name = get_model_name(token, dev_ws, dev_model, logger)
            prod_name = get_model_name(token, prod_ws, prod_model, logger)
            dev_used, dev_alloc = get_workspace_usage(token, dev_ws, logger)
            prod_used, prod_alloc = get_workspace_usage(token, prod_ws, logger)
            entry.update({
                "dev_model_name": dev_name,
                "prod_model_name": prod_name,
                "dev_ws_used": dev_used,
                "dev_ws_alloc": dev_alloc,
                "prod_ws_used": prod_used,
                "prod_ws_alloc": prod_alloc
            })
        except Exception as e:
            logger.error(f"Failed to fetch metadata for pair #{idx}: {e}")
            entry["error"] = str(e)
        enriched.append(entry)

    # 6) Display table
    rows = []
    for e in enriched:
        if e.get("error"):
            rows.append([e["index"], e.get("dev_model_id"), "<error>", e.get("dev_workspace_id"), "<error>", "<error>", e.get("prod_model_id"), "<error>", e.get("prod_workspace_id"), "<error>"])
        else:
            rows.append([
                e["index"],
                e["dev_model_id"],
                e["dev_model_name"],
                e["dev_workspace_id"],
                f"{bytes_to_human(e['dev_ws_used'])} / {bytes_to_human(e['dev_ws_alloc'])}",
                e["prod_model_id"],
                e["prod_model_name"],
                e["prod_workspace_id"],
                f"{bytes_to_human(e['prod_ws_used'])} / {bytes_to_human(e['prod_ws_alloc'])}"
            ])
    headers = ["#", "DEV Model ID", "DEV Name", "DEV WS ID", "DEV WS Used/Alloc", "PROD Model ID", "PROD Name", "PROD WS ID", "PROD WS Used/Alloc"]
    print("\nDetected pairs:\n")
    print(tabulate(rows, headers=headers, tablefmt="grid"))

    # 7) Prompt RT option per pair
    planned = []
    for e in enriched:
        if e.get("error"):
            logger.warning(f"Skipping pair #{e['index']} due to metadata error.")
            planned.append({"index": e["index"], "action": "skip", "reason": f"metadata error: {e['error']}"})
            continue
        print("\n" + "="*60)
        print(f"Pair #{e['index']}: DEV {e['dev_model_id']} ({e['dev_model_name']}) -> PROD {e['prod_model_id']} ({e['prod_model_name']})")
        action, revision = prompt_for_rt_option(token, e["dev_workspace_id"], e["dev_model_id"], logger)
        planned.append({
            "index": e["index"],
            "dev_workspace_id": e["dev_workspace_id"],
            "dev_model_id": e["dev_model_id"],
            "dev_model_name": e.get("dev_model_name"),
            "prod_workspace_id": e["prod_workspace_id"],
            "prod_model_id": e["prod_model_id"],
            "prod_model_name": e.get("prod_model_name"),
            "action": action,
            "revision": revision
        })
        logger.info(f"Planned pair #{e['index']}: action={action} revision={revision}")

    # 8) Summary of planned actions
    print("\nPlanned actions (no sync yet):\n")
    sum_rows = []
    for p in planned:
        sum_rows.append([p.get("index"), p.get("dev_model_id"), p.get("dev_model_name",""), p.get("prod_model_id"), p.get("prod_model_name",""), p.get("action"), p.get("revision") or ""])
    print(tabulate(sum_rows, headers=["#", "DEV ID", "DEV Name", "PROD ID", "PROD Name", "Action", "Revision"], tablefmt="grid"))

    cont = input("\nProceed to create/promote as planned? (y/n) [n]: ").strip().lower() or "n"
    if cont != "y":
        logger.info("User cancelled promotions. Exiting.")
        print("No promotions performed.")
        sys.exit(0)

    # 9) Execute planned items
    results = []
    for p in planned:
        idx = p["index"]
        if p.get("action") == "skip":
            results.append((p, "Skipped"))
            continue

        # prepare RT name
        rt_name = p.get("revision")
        try:
            if p["action"] == "create":
                created = None
                attempts = 0
                while attempts <= RT_RETRY:
                    try:
                        created = create_revision_tag(token, p["dev_workspace_id"], p["dev_model_id"], rt_name, logger)
                        break
                    except Exception as e:
                        attempts += 1
                        logger.warning(f"Create RT attempt {attempts}/{RT_RETRY} failed: {e}")
                        time.sleep(POLL_DELAY)
                if not created:
                    raise RuntimeError("Failed to create RT after retries")
                if isinstance(created, dict):
                    rt_name = created.get("name") or rt_name
                else:
                    rt_name = str(created)
                logger.info(f"Created RT '{rt_name}' for pair #{idx}")

            elif p["action"] == "latest":
                revs = list_revision_tags(token, p["dev_workspace_id"], p["dev_model_id"], logger)
                if not revs:
                    raise RuntimeError("No RTs found for 'latest' option")
                rt_name = revs[-1].get("name")
                logger.info(f"Using latest RT '{rt_name}' for pair #{idx}")

            elif p["action"] == "select":
                if not rt_name:
                    raise RuntimeError("No revision provided for select option")
                logger.info(f"Using selected RT '{rt_name}' for pair #{idx}")
            else:
                raise RuntimeError(f"Unknown action: {p['action']}")
        except Exception as e:
            logger.error(f"Preparation failed for pair #{idx}: {e}")
            results.append((p, f"Preparation failed: {e}"))
            continue

        # show prod workspace usage and confirm
        try:
            prod_used, prod_alloc = get_workspace_usage(token, p["prod_workspace_id"], logger)
            print(f"\nPair #{idx} PROD usage BEFORE promote: {bytes_to_human(prod_used)} / {bytes_to_human(prod_alloc)}")
            logger.info(f"Prod usage before promote for pair #{idx}: used={prod_used} alloc={prod_alloc}")
        except Exception as e:
            logger.error(f"Could not fetch prod usage for pair #{idx}: {e}")
            results.append((p, f"Failed to fetch prod usage: {e}"))
            continue

        confirm = input(f"Confirm promote RT '{rt_name}' from DEV {p['dev_model_id']} to PROD {p['prod_model_id']}? (y/n) [n]: ").strip().lower() or "n"
        if confirm != "y":
            logger.info(f"User skipped promotion for pair #{idx}")
            results.append((p, "User canceled"))
            continue

        # attempt promote (revision-promote, fallback to actions)
        try:
            promote_result = promote_revision_classic(token, p["dev_model_id"], p["dev_workspace_id"], p["prod_model_id"], p["prod_workspace_id"], rt_name, logger)
            results.append((p, "Promote initiated"))
            logger.info(f"Promote result for pair #{idx}: {promote_result}")
        except Exception as e:
            logger.error(f"Promotion failed for pair #{idx}: {e}")
            results.append((p, f"Promote failed: {e}"))

    # 10) Final summary
    print("\nFinal Results:\n")
    final_rows = []
    for r in results:
        p, status = r
        final_rows.append([p.get("index"), p.get("dev_model_id"), p.get("dev_model_name",""), p.get("prod_model_id"), p.get("prod_model_name",""), p.get("action"), p.get("revision") or "", status])
    print(tabulate(final_rows, headers=["#", "DEV ID", "DEV Name", "PROD ID", "PROD Name", "Action", "Revision", "Status"], tablefmt="grid"))

    logger.info("ALM run completed.")

if __name__ == "__main__":
    main()
