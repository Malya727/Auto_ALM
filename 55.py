#!/usr/bin/env python3
"""
auto_alm_final_classic.py

Final Classic ALM script (history export removed).
Reads config.json (Model Details -> model_ids) with workspace IDs present.
Performs: list/create/select RT in DEV, size check, promote to PROD.
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
PROMOTE_RETRY = 2

# -------------------------
# Small utilities
# -------------------------
def ts():
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def ensure_dir(p):
    if not os.path.exists(p):
        os.makedirs(p, exist_ok=True)

def bytes_to_human_select(nbytes):
    """If <1GB show MB, else show GB (as requested)."""
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
# Auth & config
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
            logger.error(f"Auth response missing token: {r.text}")
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
    model_ids = cfg.get("Model Details", {}).get("model_ids", [])
    if not model_ids:
        logger.error("No model pairs found in config.json under 'Model Details'->'model_ids'")
    return model_ids

# -------------------------
# Metadata calls (classic endpoints)
# -------------------------
def get_model_name(token, model_id, logger):
    url = f"{ANAPLAN_API_BASE}/models/{model_id}"
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    logger.debug(f"GET {url}")
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json().get("name") or "<unknown>"

def get_workspace_info(token, workspace_id, logger):
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}"
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    logger.debug(f"GET {url}")
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    j = r.json()
    # workspace name + usage info if available
    name = j.get("name") or "<unknown>"
    used = j.get("usedBytes") or j.get("used") or 0
    alloc = j.get("allocatedBytes") or j.get("allocated") or 0
    return name, int(used), int(alloc)

# -------------------------
# Revision Tag API (classic revision endpoints)
# -------------------------
def list_revision_tags(token, workspace_id, model_id, logger):
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/models/{model_id}/revisions"
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    logger.debug(f"GET {url}")
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json().get("revisions", [])

def create_revision_tag(token, workspace_id, model_id, tag_name, logger):
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/models/{model_id}/revisions"
    headers = {"Authorization": f"AnaplanAuthToken {token}", "Content-Type": "application/json"}
    payload = {"name": tag_name}
    logger.info(f"Creating RT '{tag_name}' on DEV model {model_id} (ws {workspace_id})")
    r = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
    if r.status_code in (201, 202):
        try:
            return r.json()
        except Exception:
            return {"name": tag_name}
    else:
        logger.error(f"Create RT returned {r.status_code}: {r.text}")
        r.raise_for_status()

# -------------------------
# Classic actions-based fallback helpers
# -------------------------
def list_model_actions(token, workspace_id, model_id, logger):
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/models/{model_id}/actions"
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    logger.debug(f"GET {url}")
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json().get("actions", [])

def start_action_task(token, workspace_id, model_id, action_id, payload, logger):
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/models/{model_id}/actions/{action_id}/tasks"
    headers = {"Authorization": f"AnaplanAuthToken {token}", "Content-Type": "application/json"}
    logger.debug(f"POST {url} payload={payload}")
    r = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT*2)
    if r.status_code in (200, 201, 202):
        try:
            return r.json()
        except Exception:
            return {"status_code": r.status_code}
    else:
        logger.error(f"Action task failed {r.status_code}: {r.text}")
        r.raise_for_status()

# -------------------------
# Promote (primary revision-promote, fallback to actions)
# -------------------------
def promote_revision_classic(token, dev_model_id, dev_workspace_id, prod_model_id, prod_workspace_id, revision_name, logger):
    promote_url = f"{ANAPLAN_API_BASE}/workspaces/{prod_workspace_id}/models/{prod_model_id}/revisions/promote"
    headers = {"Authorization": f"AnaplanAuthToken {token}", "Content-Type": "application/json"}
    payload = {"sourceModelId": dev_model_id, "revisionName": revision_name}
    logger.info(f"Attempting revision-promote: POST {promote_url} payload={payload}")
    try:
        r = requests.post(promote_url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT*2)
        if r.status_code in (200, 201, 202):
            try:
                return {"method": "revision_promote", "response": r.json()}
            except Exception:
                return {"method": "revision_promote", "response": {"status_code": r.status_code}}
        else:
            logger.warning(f"Revision-promote returned {r.status_code}: {r.text[:1000]}")
    except requests.HTTPError as he:
        logger.warning(f"Revision-promote HTTPError: {he}")
    except Exception as e:
        logger.warning(f"Revision-promote failed: {e}")

    logger.info("Falling back to actions-based promote (classic).")
    # fallback to actions
    try:
        actions = list_model_actions(token, prod_workspace_id, prod_model_id, logger)
    except Exception as e:
        logger.error(f"Failed to list actions on prod model: {e}")
        raise RuntimeError(f"Promote failed: cannot list actions on prod model: {e}")

    candidate = None
    for act in actions:
        name = (act.get("name") or "").lower()
        if "alm" in name or "promote" in name or "revision" in name:
            candidate = act
            break
    if not candidate and actions:
        candidate = actions[0]

    if not candidate:
        raise RuntimeError("No actions found on PROD model to perform promote")

    action_id = candidate.get("id")
    logger.info(f"Using action id={action_id} ('{candidate.get('name')}') to start task for promote")
    action_payload = {"sourceModelId": dev_model_id, "revisionName": revision_name}
    task_res = start_action_task(token, prod_workspace_id, prod_model_id, action_id, action_payload, logger)
    return {"method": "actions_task", "action_id": action_id, "response": task_res}

# -------------------------
# Interactive prompts
# -------------------------
def prompt_rt_option(token, dev_ws, dev_model, logger):
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

        # choices 2 & 3 require listing RTs
        attempts = 0
        revs = None
        while attempts <= RT_RETRY:
            try:
                revs = list_revision_tags(token, dev_ws, dev_model, logger)
                break
            except Exception as e:
                attempts += 1
                logger.warning(f"List RTs attempt {attempts} failed: {e}")
                time.sleep(POLL_DELAY)
        if revs is None:
            print("Could not list Revision Tags. Choose again or skip.")
            cont = input("Retry choosing option? (y/n) [y]: ").strip().lower() or "y"
            if cont != "y":
                return "skip", None
            continue

        if choice == "2":
            if not revs:
                print("No RTs found on DEV model. Choose create or skip.")
                continue
            latest = revs[-1].get("name")
            return "latest", latest

        if choice == "3":
            if not revs:
                print("No RTs found on DEV model.")
                create_now = input("Create new RT now? (y/n) [y]: ").strip().lower() or "y"
                if create_now == "y":
                    name = input("Enter new RT name (leave blank to auto-generate): ").strip()
                    if not name:
                        name = f"AUTO_RT_{ts()}"
                    return "create", name
                else:
                    return "skip", None
            names = [r.get("name") for r in revs]
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
            # canceled, go back to top

# -------------------------
# Main flow
# -------------------------
def main():
    # archive old logs
    temp_logger = logging.getLogger("TEMP")
    temp_logger.addHandler(logging.StreamHandler(sys.stdout))
    archive_old_logs(temp_logger)

    logger = setup_logger()

    # load config
    try:
        model_pairs = load_config(logger)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)
    if not model_pairs:
        logger.error("No model pairs; exiting.")
        sys.exit(1)

    # authenticate
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

    # enrich pairs with model & workspace names + usage (using provided workspace ids)
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
            dev_name = get_model_name(token, dev_model, logger)
            prod_name = get_model_name(token, prod_model, logger)
            dev_ws_name, dev_used, dev_alloc = get_workspace_info(token, dev_ws, logger)
            prod_ws_name, prod_used, prod_alloc = get_workspace_info(token, prod_ws, logger)
            entry.update({
                "dev_model_name": dev_name,
                "prod_model_name": prod_name,
                "dev_ws_name": dev_ws_name,
                "dev_ws_used": dev_used,
                "dev_ws_alloc": dev_alloc,
                "prod_ws_name": prod_ws_name,
                "prod_ws_used": prod_used,
                "prod_ws_alloc": prod_alloc
            })
        except Exception as e:
            logger.error(f"Failed to fetch metadata for pair #{idx}: {e}")
            entry["error"] = str(e)
        enriched.append(entry)

    # display table
    rows = []
    for e in enriched:
        if e.get("error"):
            rows.append([e["index"], e.get("dev_model_id"), "<error>", e.get("dev_workspace_id"), "<error>", "<error>", e.get("prod_model_id"), "<error>", e.get("prod_workspace_id"), "<error>"])
        else:
            rows.append([
                e["index"],
                e["dev_model_id"],
                e["dev_model_name"],
                e["dev_ws_name"],
                f"{bytes_to_human_select(e['dev_ws_used'])} / {bytes_to_human_select(e['dev_ws_alloc'])}",
                e["prod_model_id"],
                e["prod_model_name"],
                e["prod_ws_name"],
                f"{bytes_to_human_select(e['prod_ws_used'])} / {bytes_to_human_select(e['prod_ws_alloc'])}"
            ])
    headers = ["#", "DEV Model ID", "DEV Name", "DEV Workspace", "DEV Used/Alloc", "PROD Model ID", "PROD Name", "PROD Workspace", "PROD Used/Alloc"]
    print("\nDetected pairs:\n")
    print(tabulate(rows, headers=headers, tablefmt="grid"))

    # prompt RT option for each pair
    planned = []
    for e in enriched:
        if e.get("error"):
            logger.warning(f"Skipping pair #{e['index']} due to error: {e.get('error')}")
            planned.append({"index": e["index"], "action": "skip", "reason": e.get("error")})
            continue
        print("\n" + "="*60)
        print(f"Pair #{e['index']}: DEV {e['dev_model_id']} ({e['dev_model_name']}) -> PROD {e['prod_model_id']} ({e['prod_model_name']})")
        action, revision = prompt_rt_option(token, e["dev_workspace_id"], e["dev_model_id"], logger)
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

    # show planned summary
    summary_rows = []
    for p in planned:
        summary_rows.append([p.get("index"), p.get("dev_model_id"), p.get("dev_model_name",""), p.get("prod_model_id"), p.get("prod_model_name",""), p.get("action"), p.get("revision") or ""])
    print("\nPlanned actions (no sync yet):\n")
    print(tabulate(summary_rows, headers=["#", "DEV ID", "DEV Name", "PROD ID", "PROD Name", "Action", "Revision"], tablefmt="grid"))

    proceed = input("\nProceed to perform promotions for planned pairs? (y/n) [n]: ").strip().lower() or "n"
    if proceed != "y":
        logger.info("User canceled promotions. Exiting.")
        print("No promotions performed.")
        sys.exit(0)

    # execute planned items
    results = []
    for p in planned:
        idx = p.get("index")
        if p.get("action") == "skip":
            results.append((p, "Skipped"))
            continue

        # prepare revision name
        rt_name = p.get("revision")
        try:
            if p["action"] == "create":
                created = None
                attempt = 0
                while attempt <= RT_RETRY:
                    try:
                        created = create_revision_tag(token, p["dev_workspace_id"], p["dev_model_id"], rt_name, logger)
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

            elif p["action"] == "latest":
                revs = list_revision_tags(token, p["dev_workspace_id"], p["dev_model_id"], logger)
                if not revs:
                    raise RuntimeError("No RTs found for 'latest' option")
                rt_name = revs[-1].get("name")
                logger.info(f"Using latest RT '{rt_name}' for pair #{idx}")

            elif p["action"] == "select":
                if not rt_name:
                    raise RuntimeError("No revision provided for select")
                logger.info(f"Using selected RT '{rt_name}' for pair #{idx}")
            else:
                raise RuntimeError(f"Unknown action: {p['action']}")
        except Exception as e:
            logger.error(f"Preparation failed for pair #{idx}: {e}")
            results.append((p, f"Preparation failed: {e}"))
            continue

        # show prod size and confirm
        try:
            prod_used, prod_alloc = get_workspace_info(token, p["prod_workspace_id"], logger)[1:]
            print(f"\nPair #{idx} PROD workspace BEFORE promote: {bytes_to_human_select(prod_used)} / {bytes_to_human_select(prod_alloc)}")
            logger.info(f"Prod usage for pair #{idx}: used={prod_used} alloc={prod_alloc}")
        except Exception as e:
            logger.error(f"Failed to fetch prod usage for pair #{idx}: {e}")
            results.append((p, f"Failed to fetch prod usage: {e}"))
            continue

        confirm = input(f"Confirm promote RT '{rt_name}' from DEV {p['dev_model_id']} to PROD {p['prod_model_id']}? (y/n) [n]: ").strip().lower() or "n"
        if confirm != "y":
            logger.info(f"User canceled promotion for pair #{idx}")
            results.append((p, "User canceled"))
            continue

        # promote
        try:
            promote_res = promote_revision_classic(token, p["dev_model_id"], p["dev_workspace_id"], p["prod_model_id"], p["prod_workspace_id"], rt_name, logger)
            results.append((p, "Promote initiated"))
            logger.info(f"Promote result for pair #{idx}: {promote_res}")
        except Exception as e:
            logger.error(f"Promote failed for pair #{idx}: {e}")
            results.append((p, f"Promote failed: {e}"))

    # final summary
    print("\nFinal Results:\n")
    final_rows = []
    for r in results:
        p, status = r
        final_rows.append([p.get("index"), p.get("dev_model_id"), p.get("dev_model_name",""), p.get("prod_model_id"), p.get("prod_model_name",""), p.get("action"), p.get("revision") or "", status])
    print(tabulate(final_rows, headers=["#", "DEV ID", "DEV Name", "PROD ID", "PROD Name", "Action", "Revision", "Status"], tablefmt="grid"))

    logger.info("ALM run completed.")

if __name__ == "__main__":
    main()
