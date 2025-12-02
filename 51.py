#!/usr/bin/env python3
"""
auto_alm_revision_api.py

ALM automation focused on Revision API (Option A).
- Archives old logs to Log_Backup/
- Creates new log AUTO_ALM_<ts>.log in Logs/
- Authenticates with Basic Auth -> AnaplanAuthToken
- Reads config.json for dev->prod model pairs
- Auto-discovers workspace IDs & model names by scanning tenant (avoids 404)
- Shows table of pairs with model/workspace names and usage
- For each pair, lets user choose: create RT (immediate), use latest RT, list & select RT, or skip
- Collects choices for all pairs, shows summary, then performs syncs:
    - re-checks prod workspace usage
    - asks final confirmation per pair
    - promotes via /workspaces/{prodWs}/models/{prodModel}/revisions/promote
- Detailed logging for each step
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
RT_RETRY = 2  # retries for create/list RT

# -------------------------
# Utilities
# -------------------------
def timestamp():
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def bytes_to_human(nbytes):
    try:
        n = int(nbytes)
    except Exception:
        return "0 MB"
    gb = 1024**3
    mb = 1024**2
    if n >= gb:
        return f"{n/gb:.2f} GB"
    else:
        return f"{n/mb:.2f} MB"

# -------------------------
# Logging
# -------------------------
def archive_old_logs(logger=None):
    ensure_dir(LOG_BACKUP_DIR)
    for f in os.listdir("."):
        if f.endswith(".log"):
            try:
                shutil.move(f, os.path.join(LOG_BACKUP_DIR, f))
                if logger:
                    logger.debug(f"Archived {f} -> {LOG_BACKUP_DIR}")
            except Exception as e:
                if logger:
                    logger.warning(f"Could not archive {f}: {e}")

def setup_logger():
    ensure_dir(LOG_DIR)
    logname = f"AUTO_ALM_{timestamp()}.log"
    logfile = os.path.join(LOG_DIR, logname)

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
            logger.error(f"No token returned. Response: {r.text}")
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
        logger.error(f"Missing config file: {CONFIG_FILE}")
        raise FileNotFoundError(CONFIG_FILE)
    with open(CONFIG_FILE, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)
    model_pairs = cfg.get("Model Details", {}).get("model_ids", [])
    logger.debug(f"Loaded config with {len(model_pairs)} pairs")
    return model_pairs

# -------------------------
# Workspace / Model helpers (auto discovery)
# -------------------------
def find_workspace_for_model(token, model_id, logger):
    """
    Scan tenant workspaces and their models to find the workspace containing model_id.
    Returns (workspace_id, workspace_name)
    """
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    url = f"{ANAPLAN_API_BASE}/workspaces"
    logger.debug("Fetching workspaces for discovery...")
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    workspaces = r.json().get("workspaces", [])
    for ws in workspaces:
        ws_id = ws.get("id")
        ws_name = ws.get("name")
        # list models in workspace
        models_url = f"{ANAPLAN_API_BASE}/workspaces/{ws_id}/models"
        r2 = requests.get(models_url, headers=headers, timeout=REQUEST_TIMEOUT)
        r2.raise_for_status()
        models = r2.json().get("models", [])
        for m in models:
            if m.get("id") == model_id:
                logger.debug(f"Found model {model_id} in workspace {ws_id} ({ws_name})")
                return ws_id, ws_name
    raise RuntimeError(f"Workspace not found for model {model_id}")

def get_model_name(token, workspace_id, model_id, logger):
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/models/{model_id}"
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json().get("name") or "<unknown>"

def get_workspace_usage(token, workspace_id, logger):
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/usage"
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    used = data.get("usedBytes") or data.get("used") or 0
    alloc = data.get("allocatedBytes") or data.get("allocated") or 0
    return int(used), int(alloc)

# -------------------------
# Revision Tag functions (Revision API - Option A)
# -------------------------
def list_revision_tags(token, workspace_id, model_id, logger):
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/models/{model_id}/revisions"
    logger.debug(f"Listing RTs: {url}")
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json().get("revisions", [])

def create_revision_tag(token, workspace_id, model_id, tag_name, logger):
    headers = {"Authorization": f"AnaplanAuthToken {token}", "Content-Type": "application/json"}
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/models/{model_id}/revisions"
    payload = {"name": tag_name}
    logger.info(f"Creating RT '{tag_name}' on model {model_id} (ws {workspace_id})")
    r = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
    # 201/202 indicate creation accepted
    if r.status_code in (201, 202):
        try:
            return r.json()
        except Exception:
            return {"name": tag_name}
    else:
        logger.error(f"Create RT failed: {r.status_code} {r.text}")
        r.raise_for_status()

def promote_revision_tag(token, dev_model_id, prod_workspace_id, prod_model_id, revision_name, logger):
    headers = {"Authorization": f"AnaplanAuthToken {token}", "Content-Type": "application/json"}
    url = f"{ANAPLAN_API_BASE}/workspaces/{prod_workspace_id}/models/{prod_model_id}/revisions/promote"
    payload = {"sourceModelId": dev_model_id, "revisionName": revision_name}
    logger.info(f"Promote request: devModel={dev_model_id} -> prodModel={prod_model_id} using RT='{revision_name}'")
    r = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT*2)
    if r.status_code in (200, 201, 202):
        try:
            return r.json()
        except Exception:
            return {"status_code": r.status_code}
    else:
        logger.error(f"Promote failed: {r.status_code} {r.text}")
        r.raise_for_status()

# -------------------------
# Interactive helpers
# -------------------------
def prompt_rt_choice(token, dev_ws, dev_model, logger):
    """
    Interactively choose RT action for a single DEV model.
    Returns tuple (action, revision_name_or_None)
    action in {"create","latest","select","skip"}
    """
    while True:
        print("\nRT options for DEV model:", dev_model)
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
            rt_name = input("Enter new RT name (leave blank to auto-generate): ").strip()
            if not rt_name:
                rt_name = f"AUTO_RT_{timestamp()}"
            return "create", rt_name

        # for 2 and 3 we need to list RTs (with retry)
        retries = 0
        while True:
            try:
                revisions = list_revision_tags(token, dev_ws, dev_model, logger)
                break
            except Exception as e:
                retries += 1
                logger.warning(f"Failed to list RTs (attempt {retries}/{RT_RETRY}): {e}")
                if retries >= RT_RETRY:
                    print("Could not list Revision Tags. You can retry choice or skip.")
                    retry = input("Retry listing? (y/n): ").strip().lower()
                    if retry == "y":
                        retries = 0
                        continue
                    else:
                        return "skip", None
                time.sleep(POLL_DELAY)

        if choice == "2":
            if not revisions:
                print("No existing RTs found on DEV model. Choose create or skip.")
                continue
            # assume last is latest
            latest = revisions[-1].get("name")
            return "latest", latest

        if choice == "3":
            if not revisions:
                print("No existing RTs found on DEV model.")
                create_now = input("Create new RT now? (y/n) [y]: ").strip().lower() or "y"
                if create_now == "y":
                    rt_name = input("Enter new RT name (leave blank to auto-generate): ").strip()
                    if not rt_name:
                        rt_name = f"AUTO_RT_{timestamp()}"
                    return "create", rt_name
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
                    print("Selection out of range.")
                    continue
                return "select", names[idx]
            # if cancelled, go back to top of RT options

# -------------------------
# Main flow
# -------------------------
def main():
    # 1. Archive old logs
    temp_logger = logging.getLogger("AUTO_ALM_TEMP")
    temp_logger.addHandler(logging.StreamHandler(sys.stdout))
    archive_old_logs(temp_logger)

    # 2. Setup logger
    logger = setup_logger()

    # 3. Load config
    try:
        model_pairs = load_config(logger)
    except Exception as e:
        logger.error(f"Could not load config: {e}")
        sys.exit(1)
    if not model_pairs:
        logger.error("No model pairs found in config.json")
        sys.exit(1)

    # 4. Authenticate
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

    # 5. Discover metadata for all pairs
    pairs_info = []
    logger.info("Discovering workspace/model metadata for all pairs...")
    for idx, pair in enumerate(model_pairs, start=1):
        dev_id = pair.get("dev_model_id")
        prod_id = pair.get("prod_model_id")
        entry = {"pair_index": idx, "dev_model_id": dev_id, "prod_model_id": prod_id}
        try:
            dev_ws_id, dev_ws_name = find_workspace_for_model(token, dev_id, logger)
            prod_ws_id, prod_ws_name = find_workspace_for_model(token, prod_id, logger)
            dev_model_name = get_model_name(token, dev_ws_id, dev_id, logger)
            prod_model_name = get_model_name(token, prod_ws_id, prod_id, logger)
            dev_used, dev_alloc = get_workspace_usage(token, dev_ws_id, logger)
            prod_used, prod_alloc = get_workspace_usage(token, prod_ws_id, logger)
            entry.update({
                "dev_ws_id": dev_ws_id, "dev_ws_name": dev_ws_name, "dev_model_name": dev_model_name,
                "dev_ws_used": dev_used, "dev_ws_alloc": dev_alloc,
                "prod_ws_id": prod_ws_id, "prod_ws_name": prod_ws_name, "prod_model_name": prod_model_name,
                "prod_ws_used": prod_used, "prod_ws_alloc": prod_alloc
            })
            logger.debug(f"Pair #{idx} discovered: DEV {dev_id} ({dev_model_name}) in ws {dev_ws_id}; PROD {prod_id} ({prod_model_name}) in ws {prod_ws_id}")
        except Exception as e:
            logger.error(f"Discovery failed for pair #{idx} (dev={dev_id}, prod={prod_id}): {e}")
            entry["error"] = str(e)
        pairs_info.append(entry)

    # 6. Show table
    rows = []
    for p in pairs_info:
        if p.get("error"):
            rows.append([p["pair_index"], p.get("dev_model_id"), "<error>", "<error>", "<error>", "<error>", p.get("prod_model_id"), "<error>", "<error>", "<error>", "<error>"])
        else:
            rows.append([
                p["pair_index"],
                p["dev_model_id"],
                p["dev_model_name"],
                p["dev_ws_id"],
                p["dev_ws_name"],
                f"{bytes_to_human(p['dev_ws_used'])} / {bytes_to_human(p['dev_ws_alloc'])}",
                p["prod_model_id"],
                p["prod_model_name"],
                p["prod_ws_id"],
                p["prod_ws_name"],
                f"{bytes_to_human(p['prod_ws_used'])} / {bytes_to_human(p['prod_ws_alloc'])}"
            ])
    headers = ["#", "DEV ID", "DEV Name", "DEV WS ID", "DEV WS Name", "DEV WS Used/Alloc", "PROD ID", "PROD Name", "PROD WS ID", "PROD WS Name", "PROD WS Used/Alloc"]
    print("\nDetected DEV -> PROD pairs:\n")
    print(tabulate(rows, headers=headers, tablefmt="grid"))

    # 7. Prompt user for each pair (collect actions)
    planned = []
    for p in pairs_info:
        if p.get("error"):
            logger.warning(f"Skipping pair #{p['pair_index']} due to discovery error.")
            continue
        print("\n" + "="*60)
        print(f"Pair #{p['pair_index']}: DEV {p['dev_model_id']} -> PROD {p['prod_model_id']}")
        print(f"DEV Model Name: {p['dev_model_name']} (WS {p['dev_ws_id']} - {p['dev_ws_name']})")
        print(f"PROD Model Name: {p['prod_model_name']} (WS {p['prod_ws_id']} - {p['prod_ws_name']})")
        action, revision = prompt_rt_choice(token, p["dev_ws_id"], p["dev_model_id"], logger)
        planned.append({
            "pair_index": p["pair_index"],
            "dev_model_id": p["dev_model_id"],
            "dev_ws_id": p["dev_ws_id"],
            "dev_model_name": p.get("dev_model_name"),
            "prod_model_id": p["prod_model_id"],
            "prod_ws_id": p["prod_ws_id"],
            "prod_model_name": p.get("prod_model_name"),
            "action": action,
            "revision": revision
        })
        logger.info(f"Planned for pair #{p['pair_index']}: action={action} revision={revision}")

    # 8. Summary of planned actions
    print("\nPlanned actions (no sync yet):\n")
    sum_rows = []
    for a in planned:
        sum_rows.append([a["pair_index"], a["dev_model_id"], a["dev_model_name"], a["prod_model_id"], a["prod_model_name"], a["action"], a["revision"] or ""])
    print(tabulate(sum_rows, headers=["#", "DEV ID", "DEV Name", "PROD ID", "PROD Name", "Action", "Revision"], tablefmt="grid"))

    # Confirm proceed to perform syncs
    launch = input("\nProceed to perform promotions for planned pairs? (y/n) [n]: ").strip().lower() or "n"
    if launch != "y":
        logger.info("User chose not to proceed with promotions. Exiting.")
        print("No promotions performed. Exiting.")
        sys.exit(0)

    # 9. Execute planned actions (create RT if needed, then promote after showing prod size)
    results = []
    for a in planned:
        idx = a["pair_index"]
        if a["action"] == "skip":
            results.append((a, "Skipped by user"))
            continue

        # prepare RT name
        rt_name = a["revision"]  # may be None for 'latest'
        try:
            if a["action"] == "create":
                # create now
                created = None
                attempt = 0
                while attempt <= RT_RETRY:
                    try:
                        created = create_revision_tag(token, a["dev_ws_id"], a["dev_model_id"], rt_name, logger)
                        break
                    except Exception as e:
                        attempt += 1
                        logger.warning(f"Attempt {attempt}/{RT_RETRY} create RT failed: {e}")
                        time.sleep(POLL_DELAY)
                if not created:
                    raise RuntimeError("Failed to create RT after retries")
                # created may be dict or name
                if isinstance(created, dict):
                    rt_name = created.get("name") or rt_name
                else:
                    rt_name = str(created)
                logger.info(f"Created RT '{rt_name}' for pair #{idx}")

            elif a["action"] == "latest":
                revs = list_revision_tags(token, a["dev_ws_id"], a["dev_model_id"], logger)
                if not revs:
                    raise RuntimeError("No revisions found on DEV model for 'latest' option")
                rt_name = revs[-1].get("name")  # assume last is latest
                logger.info(f"Using latest RT '{rt_name}' for pair #{idx}")

            elif a["action"] == "select":
                # revision already selected earlier
                if not rt_name:
                    raise RuntimeError("No revision provided for 'select' action")
                logger.info(f"Selected RT '{rt_name}' for pair #{idx}")

            else:
                raise RuntimeError(f"Unknown action {a['action']}")

        except Exception as e:
            logger.error(f"Preparation failed for pair #{idx}: {e}")
            results.append((a, f"Preparation failed: {e}"))
            continue

        # Re-check prod workspace usage and show to user
        try:
            prod_used, prod_alloc = get_workspace_usage(token, a["prod_ws_id"], logger)
            print(f"\nPair #{idx} PROD workspace usage: {bytes_to_human(prod_used)} / {bytes_to_human(prod_alloc)}")
            logger.info(f"Prod usage before promote: used={prod_used} alloc={prod_alloc}")
        except Exception as e:
            logger.error(f"Could not fetch PROD workspace usage for pair #{idx}: {e}")
            results.append((a, f"Failed to fetch prod usage: {e}"))
            continue

        confirm = input(f"Confirm promote RT '{rt_name}' DEV->{a['prod_model_id']}? (y/n) [n]: ").strip().lower() or "n"
        if confirm != "y":
            logger.info(f"User skipped promotion for pair #{idx}")
            results.append((a, "User canceled"))
            continue

        # Promote
        try:
            resp = promote_revision_tag(token, a["dev_model_id"], a["prod_ws_id"], a["prod_model_id"], rt_name, logger)
            results.append((a, "Promote initiated"))
            logger.info(f"Promotion success for pair #{idx}: {resp}")
        except Exception as e:
            logger.error(f"Promotion failed for pair #{idx}: {e}")
            results.append((a, f"Promote failed: {e}"))

    # 10. Final result summary
    print("\nFinal Results:\n")
    final_rows = []
    for r in results:
        a, status = r
        final_rows.append([a["pair_index"], a["dev_model_id"], a.get("dev_model_name",""), a["prod_model_id"], a.get("prod_model_name",""), a["action"], a.get("revision") or "", status])
    print(tabulate(final_rows, headers=["#", "DEV ID", "DEV Name", "PROD ID", "PROD Name", "Action", "Revision", "Status"], tablefmt="grid"))

    logger.info("ALM run finished.")

if __name__ == "__main__":
    main()
