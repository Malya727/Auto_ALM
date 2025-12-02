#!/usr/bin/env python3
"""
auto_alm_final.py

Final ALM automation script using workspace IDs from config.json.
Implements 3 RT options (create/latest/select), size checks, and promotion.
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

# optional nicer password input
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

# -------------------------
# Utilities
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
# Logging helpers
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
# Auth / config
# -------------------------
def authenticate(username, password, logger):
    logger.info("Authenticating to Anaplan...")
    try:
        r = requests.post(ANAPLAN_AUTH_URL, auth=HTTPBasicAuth(username, password), timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        logger.error(f"Authentication request failed: {e}")
        raise
    try:
        token = r.json().get("tokenInfo", {}).get("tokenValue")
        if not token:
            logger.error(f"Auth success but token missing: {r.text}")
            raise RuntimeError("Token missing in auth response")
        logger.info("Authentication successful.")
        return token
    except Exception as e:
        logger.error(f"Failed parsing auth response: {e}")
        raise

def load_config(logger):
    if not os.path.exists(CONFIG_FILE):
        logger.error(f"Config file not found: {CONFIG_FILE}")
        raise FileNotFoundError(CONFIG_FILE)
    with open(CONFIG_FILE, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)
    pairs = cfg.get("pairs") or cfg.get("Model Details", {}).get("model_ids") or []
    if not pairs:
        logger.error("No pairs found in config.json (expected key 'pairs')")
    return pairs

# -------------------------
# Simple model & workspace helpers (using provided workspace IDs)
# -------------------------
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
    j = r.json()
    used = j.get("usedBytes") or j.get("used") or 0
    alloc = j.get("allocatedBytes") or j.get("allocated") or 0
    return int(used), int(alloc)

# -------------------------
# Revision Tag APIs (Revision API - Option A)
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
    # 201/202 indicates created/accepted
    if r.status_code in (201, 202):
        try:
            return r.json()
        except Exception:
            return {"name": tag_name}
    else:
        logger.error(f"Create RT returned {r.status_code}: {r.text}")
        r.raise_for_status()

def promote_revision_tag(token, dev_model_id, prod_workspace_id, prod_model_id, revision_name, logger):
    headers = {"Authorization": f"AnaplanAuthToken {token}", "Content-Type": "application/json"}
    url = f"{ANAPLAN_API_BASE}/workspaces/{prod_workspace_id}/models/{prod_model_id}/revisions/promote"
    payload = {"sourceModelId": dev_model_id, "revisionName": revision_name}
    logger.info(f"Promote: devModel={dev_model_id} -> prodModel={prod_model_id} using RT='{revision_name}'")
    r = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT*2)
    if r.status_code in (200, 201, 202):
        try:
            return r.json()
        except Exception:
            return {"status_code": r.status_code}
    else:
        logger.error(f"Promote returned {r.status_code}: {r.text}")
        r.raise_for_status()

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
    # Archive old logs first (temporary logger)
    temp_logger = logging.getLogger("TEMP")
    temp_logger.addHandler(logging.StreamHandler(sys.stdout))
    archive_old_logs(temp_logger)

    logger = setup_logger()

    # Load config
    try:
        pairs = load_config(logger)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)
    if not pairs:
        logger.error("No pairs defined. Exiting.")
        sys.exit(1)

    # Authenticate
    username = input("Anaplan username/email: ").strip()
    if pwinput:
        password = pwinput.pwinput("Anaplan password: ")
    else:
        password = getpass("Anaplan password: ")
    try:
        token = authenticate(username, password, logger)
    except Exception as e:
        logger.error(f"Auth failed: {e}")
        sys.exit(1)

    # Enrich pairs with model & workspace names & usage
    enriched = []
    logger.info("Fetching model & workspace names and usage for pairs...")
    for i, p in enumerate(pairs, start=1):
        dev_ws = p.get("dev_workspace_id")
        dev_model = p.get("dev_model_id")
        prod_ws = p.get("prod_workspace_id")
        prod_model = p.get("prod_model_id")
        entry = {
            "index": i,
            "dev_workspace_id": dev_ws,
            "dev_model_id": dev_model,
            "prod_workspace_id": prod_ws,
            "prod_model_id": prod_model
        }
        try:
            dev_model_name = get_model_name(token, dev_ws, dev_model, logger)
            prod_model_name = get_model_name(token, prod_ws, prod_model, logger)
            dev_used, dev_alloc = get_workspace_usage(token, dev_ws, logger)
            prod_used, prod_alloc = get_workspace_usage(token, prod_ws, logger)
            entry.update({
                "dev_model_name": dev_model_name,
                "prod_model_name": prod_model_name,
                "dev_ws_used": dev_used,
                "dev_ws_alloc": dev_alloc,
                "prod_ws_used": prod_used,
                "prod_ws_alloc": prod_alloc
            })
        except Exception as e:
            logger.error(f"Failed to fetch metadata for pair #{i}: {e}")
            entry["error"] = str(e)
        enriched.append(entry)

    # Show table of pairs
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
                bytes_to_human(e["dev_ws_used"]) + " / " + bytes_to_human(e["dev_ws_alloc"]),
                e["prod_model_id"],
                e["prod_model_name"],
                e["prod_workspace_id"],
                bytes_to_human(e["prod_ws_used"]) + " / " + bytes_to_human(e["prod_ws_alloc"])
            ])
    headers = ["#", "DEV Model ID", "DEV Model Name", "DEV WS ID", "DEV WS Used/Alloc", "PROD Model ID", "PROD Model Name", "PROD WS ID", "PROD WS Used/Alloc"]
    print("\nDetected pairs:\n")
    print(tabulate(rows, headers=headers, tablefmt="grid"))

    # Prompt RT option for each pair
    planned = []
    for e in enriched:
        if e.get("error"):
            logger.warning(f"Skipping pair #{e['index']} due to earlier metadata error.")
            continue
        print("\n" + "="*60)
        print(f"Pair #{e['index']}:")
        print(f"  DEV:  {e['dev_model_id']}  ({e['dev_model_name']})  WS:{e['dev_workspace_id']}")
        print(f"  PROD: {e['prod_model_id']}  ({e['prod_model_name']})  WS:{e['prod_workspace_id']}")
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

    # Summary of planned actions
    sum_rows = []
    for a in planned:
        sum_rows.append([a["index"], a["dev_model_id"], a.get("dev_model_name",""), a["prod_model_id"], a.get("prod_model_name",""), a["action"], a.get("revision") or ""])
    print("\nPlanned actions (no sync yet):\n")
    print(tabulate(sum_rows, headers=["#", "DEV ID", "DEV Name", "PROD ID", "PROD Name", "Action", "Revision"], tablefmt="grid"))

    proceed = input("\nProceed to perform promotions for planned pairs? (y/n) [n]: ").strip().lower() or "n"
    if proceed != "y":
        logger.info("User canceled promotions. Exiting.")
        print("No promotions performed.")
        sys.exit(0)

    # Execute planned actions
    results = []
    for a in planned:
        idx = a["index"]
        if a["action"] == "skip":
            results.append((a, "Skipped"))
            continue

        # Prepare RT name
        rt_name = a["revision"]
        try:
            if a["action"] == "create":
                created = None
                attempt = 0
                while attempt <= RT_RETRY:
                    try:
                        created = create_revision_tag(token, a["dev_workspace_id"], a["dev_model_id"], rt_name, logger)
                        break
                    except Exception as e:
                        attempt += 1
                        logger.warning(f"Create RT attempt {attempt}/{RT_RETRY} failed: {e}")
                        time.sleep(POLL_DELAY)
                if not created:
                    raise RuntimeError("Failed to create RT after retries")
                if isinstance(created, dict):
                    rt_name = created.get("name") or rt_name
                else:
                    rt_name = str(created)
                logger.info(f"Created RT '{rt_name}' for pair #{idx}")

            elif a["action"] == "latest":
                revs = list_revision_tags(token, a["dev_workspace_id"], a["dev_model_id"], logger)
                if not revs:
                    raise RuntimeError("No revisions found for 'latest' option")
                rt_name = revs[-1].get("name")
                logger.info(f"Using latest RT '{rt_name}' for pair #{idx}")

            elif a["action"] == "select":
                if not rt_name:
                    raise RuntimeError("No revision name provided for select")
                logger.info(f"Selected RT '{rt_name}' for pair #{idx}")

            else:
                raise RuntimeError(f"Unknown action {a['action']}")

        except Exception as e:
            logger.error(f"Preparation failed for pair #{idx}: {e}")
            results.append((a, f"Prep failed: {e}"))
            continue

        # Check prod usage and ask final confirmation
        try:
            prod_used, prod_alloc = get_workspace_usage(token, a["prod_workspace_id"], logger)
            print(f"\nPair #{idx} PROD workspace size: {bytes_to_human(prod_used)} / {bytes_to_human(prod_alloc)}")
            logger.info(f"Prod usage before promote: {prod_used} of {prod_alloc}")
        except Exception as e:
            logger.error(f"Could not fetch PROD usage for pair #{idx}: {e}")
            results.append((a, f"Failed to fetch prod usage: {e}"))
            continue

        confirm = input(f"Confirm promote RT '{rt_name}' from DEV {a['dev_model_id']} to PROD {a['prod_model_id']}? (y/n) [n]: ").strip().lower() or "n"
        if confirm != "y":
            logger.info(f"User skipped promotion for pair #{idx}")
            results.append((a, "User canceled"))
            continue

        # Promote
        try:
            resp = promote_revision_tag(token, a["dev_model_id"], a["prod_workspace_id"], a["prod_model_id"], rt_name, logger)
            results.append((a, "Promote initiated"))
            logger.info(f"Promote response for pair #{idx}: {resp}")
        except Exception as e:
            logger.error(f"Promote failed for pair #{idx}: {e}")
            results.append((a, f"Promote failed: {e}"))

    # Final results
    print("\nFinal Results:\n")
    final_rows = []
    for r in results:
        a, status = r
        final_rows.append([a["index"], a["dev_model_id"], a.get("dev_model_name",""), a["prod_model_id"], a.get("prod_model_name",""), a["action"], a.get("revision") or "", status])
    print(tabulate(final_rows, headers=["#", "DEV ID", "DEV Name", "PROD ID", "PROD Name", "Action", "Revision", "Status"], tablefmt="grid"))

    logger.info("ALM run completed.")

if __name__ == "__main__":
    main()
