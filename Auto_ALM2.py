#!/usr/bin/env python3
"""
Auto ALM - final script
- Auto-discovers workspace/model names (Option B)
- Table view for each Dev->Prod pair
- Size formatting: <1GB -> MB, >=1GB -> GB
- Parallel sync execution
Dependencies: requests (pip install requests), optional tabulate (pip install tabulate)
"""

import os
import sys
import json
import time
import shutil
import logging
from datetime import datetime
from getpass import getpass
from typing import List, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor
import requests

# -------------------------
# Constants & defaults
# -------------------------
CONFIG_FILE = "config.json"
LOG_BACKUP_DIR = "Log_Backup"
LOGS_DIR = "Logs"
MODEL_HISTORY_DIR = "Model_History"
LOG_FILENAME_PREFIX = "Auto_ALM"
MODEL_HISTORY_PREFIX = "MODEL_HISTORY"
ANAPLAN_BASE = "https://api.anaplan.com"  # adjust if needed
REQUEST_TIMEOUT = 30
MAX_WORKERS = 6  # threads for parallel sync

# Attempt to import tabulate; if missing use fallback
try:
    from tabulate import tabulate
    HAVE_TABULATE = True
except Exception:
    HAVE_TABULATE = False

# ANSI color helpers (simple and cross-platform safe for most terminals)
class Colors:
    OK = "\033[92m"
    WARN = "\033[93m"
    ERR = "\033[91m"
    BLUE = "\033[94m"
    RESET = "\033[0m"

# -------------------------
# Utilities
# -------------------------
def timestamp_str() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def bytes_to_human(nbytes: int) -> str:
    """
    If < 1 GB => show in MB with 2 decimals, else show in GB with 2 decimals.
    1 GB = 1024**3 bytes
    1 MB = 1024**2 bytes
    """
    one_gb = 1024 ** 3
    one_mb = 1024 ** 2
    if nbytes < one_gb:
        mb = nbytes / one_mb
        return f"{mb:,.2f} MB"
    else:
        gb = nbytes / one_gb
        return f"{gb:,.2f} GB"

# -------------------------
# Logging & archival
# -------------------------
def archive_existing_logs(main_dir: str = ".", backup_dir: str = LOG_BACKUP_DIR) -> List[str]:
    moved = []
    ensure_dir(backup_dir)
    # If backup_dir was just created, notify
    if len(os.listdir(backup_dir)) == 0:
        print(f"{Colors.BLUE}Created folder: {backup_dir}{Colors.RESET}")
    for entry in os.listdir(main_dir):
        path = os.path.join(main_dir, entry)
        if os.path.isfile(path) and entry.lower().endswith(".log"):
            dest = os.path.join(backup_dir, entry)
            shutil.move(path, dest)
            print(f"Moved log file '{entry}' -> '{backup_dir}'")
            moved.append(dest)
    return moved

def create_new_log(logs_dir: str = LOGS_DIR) -> Tuple[str, logging.Logger]:
    ensure_dir(logs_dir)
    fname = f"{LOG_FILENAME_PREFIX}_{timestamp_str()}.log"
    path = os.path.join(logs_dir, fname)

    logger = logging.getLogger("auto_alm")
    logger.setLevel(logging.DEBUG)
    # Clear existing handlers
    logger.handlers.clear()

    fh = logging.FileHandler(path)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(ch)

    logger.info(f"Log file created: {path}")
    return path, logger

# -------------------------
# Config & inputs
# -------------------------
def load_config(path: str = CONFIG_FILE) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def parse_model_pairs(config: dict) -> Tuple[str, List[Dict[str, str]]]:
    md = config.get("Model Details", {})
    export_action_name = md.get("export_action_name", "").strip()
    pairs = []
    for ent in md.get("model_ids", []):
        dev = ent.get("dev_model_id")
        prod = ent.get("prod_model_id")
        if dev and prod:
            pairs.append({"dev": dev, "prod": prod})
    return export_action_name, pairs

def prompt_credentials() -> Tuple[str, str]:
    user = input("Enter Anaplan username/email: ").strip()
    pwd = getpass("Enter Anaplan password (hidden): ")
    return user, pwd

# -------------------------
# Anaplan API helpers
# -------------------------
def authenticate_anaplan(username: str, password: str, logger: logging.Logger) -> str:
    logger.info("Authenticating to Anaplan...")
    url = f"{ANAPLAN_BASE}/2/0/authenticate"
    try:
        resp = requests.post(url, auth=(username, password), timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            token = None
            try:
                j = resp.json()
                # common token key possibilities
                token = j.get("token") or j.get("authToken") or j.get("authorizationToken")
            except Exception:
                token = None
            if not token:
                # try headers
                token = resp.headers.get("Authorization") or resp.headers.get("X-Auth-Token")
            if not token:
                logger.error("Authentication returned 200 but token not found.")
                raise RuntimeError("Auth token missing in response")
            logger.info("Authentication successful.")
            return token
        else:
            logger.error(f"Authentication failed: {resp.status_code} {resp.text}")
            raise RuntimeError("Authentication failed")
    except requests.RequestException as e:
        logger.error(f"Authentication request error: {e}")
        raise

def get_all_workspaces(token: str, logger: logging.Logger) -> List[Dict[str, Any]]:
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    url = f"{ANAPLAN_BASE}/2/0/workspaces"
    try:
        r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        # data may be {'workspaces': [...]}
        return data.get("workspaces") or data.get("items") or []
    except requests.RequestException as e:
        logger.warning(f"Could not list workspaces: {e}")
        return []

def get_models_in_workspace(token: str, workspace_id: str, logger: logging.Logger) -> List[Dict[str, Any]]:
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    url = f"{ANAPLAN_BASE}/2/0/workspaces/{workspace_id}/models"
    try:
        r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        return data.get("models") or data.get("items") or []
    except requests.RequestException as e:
        logger.warning(f"Could not list models in workspace {workspace_id}: {e}")
        return []

def discover_model_and_workspace_names(token: str, model_id_list: List[str], logger: logging.Logger) -> Dict[str, Dict[str, str]]:
    """
    For each model_id in model_id_list, find model name & workspace name.
    Returns dict keyed by model_id -> {"model_name": ..., "workspace_id": ..., "workspace_name": ...}
    """
    result = {}
    workspaces = get_all_workspaces(token, logger)
    for ws in workspaces:
        ws_id = ws.get("id") or ws.get("workspaceId") or ws.get("workspace", {}).get("id")
        ws_name = ws.get("name") or ws.get("workspace", {}).get("name")
        if not ws_id:
            continue
        models = get_models_in_workspace(token, ws_id, logger)
        for m in models:
            mid = m.get("id") or m.get("modelId")
            if not mid:
                continue
            if mid in model_id_list:
                model_name = m.get("name") or m.get("model", {}).get("name")
                result[mid] = {
                    "model_name": model_name or "(unknown)",
                    "workspace_id": ws_id,
                    "workspace_name": ws_name or "(unknown)"
                }
    # For any not found, mark unknown
    for mid in model_id_list:
        if mid not in result:
            result[mid] = {"model_name": "(unknown)", "workspace_id": None, "workspace_name": "(unknown)"}
    return result

# -------------------------
# Export & model history
# -------------------------
def find_export_id_by_name(token: str, model_id: str, export_name: str, logger: logging.Logger) -> str:
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    url = f"{ANAPLAN_BASE}/2/0/models/{model_id}/exports"
    try:
        r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        exports = data.get("exports") or data.get("items") or []
        for e in exports:
            if e.get("name") == export_name:
                return e.get("id")
        raise RuntimeError(f"Export '{export_name}' not found on model {model_id}")
    except requests.RequestException as e:
        logger.error(f"Error listing exports for model {model_id}: {e}")
        raise

def run_export_and_download(token: str, model_id: str, export_id: str, out_dir: str, logger: logging.Logger) -> str:
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    start_url = f"{ANAPLAN_BASE}/2/0/models/{model_id}/exports/{export_id}/tasks"
    try:
        r = requests.post(start_url, headers=headers, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        task = r.json().get("task") or r.json()
        task_id = task.get("id") or task.get("taskId") or task.get("task", {}).get("id")
        if not task_id:
            logger.error("Export task id missing after starting export.")
            raise RuntimeError("Task id missing")
        task_url = f"{start_url}/{task_id}"
        logger.info("Waiting for export to complete...")
        for _ in range(90):  # poll for up to ~3 minutes
            time.sleep(2)
            try:
                tr = requests.get(task_url, headers=headers, timeout=REQUEST_TIMEOUT)
                tr.raise_for_status()
                tdata = tr.json().get("task") or tr.json()
                status = tdata.get("status") or tdata.get("state")
                if status and status.lower() in ("completed", "success"):
                    break
            except Exception:
                continue
        download_url = f"{ANAPLAN_BASE}/2/0/models/{model_id}/exports/{export_id}/result"
        dl = requests.get(download_url, headers=headers, timeout=REQUEST_TIMEOUT, stream=True)
        dl.raise_for_status()
        ensure_dir(out_dir)
        filename = f"{model_id}_{timestamp_str()}.zip"
        filepath = os.path.join(out_dir, filename)
        with open(filepath, "wb") as fh:
            for chunk in dl.iter_content(chunk_size=8192):
                if chunk:
                    fh.write(chunk)
        logger.info(f"Downloaded model history to {filepath}")
        return filepath
    except requests.RequestException as e:
        logger.error(f"Export/download error for model {model_id}: {e}")
        raise

# -------------------------
# Revision tags
# -------------------------
def list_revision_tags(token: str, model_id: str, logger: logging.Logger) -> List[Dict[str, Any]]:
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    url = f"{ANAPLAN_BASE}/2/0/models/{model_id}/revisions"
    try:
        r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        return data.get("revisions") or data.get("items") or []
    except requests.RequestException:
        logger.warning("Could not list revision tags (endpoint may differ). Returning empty list.")
        return []

def create_revision_tag(token: str, model_id: str, tag_name: str, logger: logging.Logger) -> Dict[str, Any]:
    headers = {"Authorization": f"AnaplanAuthToken {token}", "Content-Type": "application/json"}
    url = f"{ANAPLAN_BASE}/2/0/models/{model_id}/revisions"
    payload = {"name": tag_name}
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        logger.error(f"Failed to create revision tag '{tag_name}' on model {model_id}: {e}")
        raise

# -------------------------
# Workspace usage & estimation
# -------------------------
def get_workspace_usage(token: str, workspace_id: str, logger: logging.Logger) -> Tuple[int, int]:
    """
    Return (used_bytes, allocated_bytes). If unavailable return (0,0).
    """
    if not workspace_id:
        return 0, 0
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    url = f"{ANAPLAN_BASE}/2/0/workspaces/{workspace_id}/usage"
    try:
        r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        d = r.json()
        used = d.get("used") or d.get("usedBytes") or d.get("usedSpace") or 0
        alloc = d.get("allocated") or d.get("allocatedBytes") or d.get("allocatedSpace") or 0
        return int(used), int(alloc)
    except requests.RequestException:
        logger.warning(f"Could not fetch usage for workspace {workspace_id}.")
        return 0, 0

def estimate_post_sync(token: str, prod_model_info: Dict[str, Any], revision_file_path: str, logger: logging.Logger) -> Dict[str, Any]:
    ws_id = prod_model_info.get("workspace_id")
    used, alloc = get_workspace_usage(token, ws_id, logger)
    revision_size = 0
    if revision_file_path and os.path.exists(revision_file_path):
        try:
            revision_size = os.path.getsize(revision_file_path)
        except Exception:
            revision_size = 0
    after = used + revision_size
    pct_after = (after / alloc) if alloc and alloc > 0 else 0.0
    return {"used": used, "alloc": alloc, "revision_size": revision_size, "after": after, "pct_after": pct_after}

# -------------------------
# UX: Table & formatting
# -------------------------
def pretty_table_for_pair(dev_info: Dict[str, str], prod_info: Dict[str, str]) -> str:
    headers = ["Information", "DEV", "PROD"]
    rows = [
        ["Model ID", dev_info.get("model_id"), prod_info.get("model_id")],
        ["Model Name", dev_info.get("model_name"), prod_info.get("model_name")],
        ["Workspace", dev_info.get("workspace_name"), prod_info.get("workspace_name")]
    ]
    if HAVE_TABULATE:
        return tabulate(rows, headers=headers, tablefmt="pretty")
    else:
        # simple fallback
        col1w = 20
        colw = 28
        sep = "+" + "-" * (col1w + 2) + "+" + "-" * (colw + 2) + "+" + "-" * (colw + 2) + "+"
        lines = [sep]
        lines.append("| {:<{}} | {:<{}} | {:<{}} |".format("Information", col1w, "DEV", colw, "PROD", colw))
        lines.append(sep)
        for r in rows:
            lines.append("| {:<{}} | {:<{}} | {:<{}} |".format(r[0], col1w, str(r[1])[:colw], colw, str(r[2])[:colw], colw))
        lines.append(sep)
        return "\n".join(lines)

# -------------------------
# Interactive per-pair flow
# -------------------------
def ask_revision_choice_for_pair(idx: int, dev_info: Dict[str, Any], prod_info: Dict[str, Any],
                                 token: str, export_action_name: str, logger: logging.Logger,
                                 revision_file_path: str) -> Dict[str, Any]:
    print("\n" + "=" * 72)
    print(f"{Colors.BLUE}Pair #{idx + 1}{Colors.RESET}")
    print(pretty_table_for_pair(dev_info, prod_info))
    print("→ Processing this Dev → Prod pair...\n")

    # 3 options for revision
    print("Select Revision Tag option:")
    print("  1) Use latest available Revision Tag")
    print("  2) Create a new Revision Tag now")
    print("  3) List available Revision Tags and select one")
    choice = input("Enter choice (1/2/3) [default 1]: ").strip() or "1"

    chosen_tag = None
    if choice == "1":
        tags = list_revision_tags(token, dev_info["model_id"], logger)
        if tags:
            # prefer named tag (take first)
            t = tags[0]
            chosen_tag = t.get("name") or t.get("id")
            print(f"Selected latest tag: {chosen_tag}")
        else:
            print("No tags found on Dev model; switching to create new tag.")
            choice = "2"

    if choice == "2":
        tag_name = input("Enter name for new revision tag (leave blank for auto-name): ").strip()
        if not tag_name:
            tag_name = f"AutoTag_{timestamp_str()}"
            print(f"No name provided. Using '{tag_name}'")
        try:
            created = create_revision_tag(token, dev_info["model_id"], tag_name, logger)
            # try extract name
            chosen_tag = created.get("name") or created.get("id") or tag_name
            print(f"{Colors.OK}Created revision tag: {chosen_tag}{Colors.RESET}")
        except Exception:
            print(f"{Colors.WARN}Warning: API create failed; storing provided name '{tag_name}' and continuing.{Colors.RESET}")
            chosen_tag = tag_name

    if choice == "3":
        tags = list_revision_tags(token, dev_info["model_id"], logger)
        if not tags:
            print("No tags found; switching to create new tag.")
            choice = "2"
            tag_name = input("Enter name for new revision tag (leave blank for auto-name): ").strip()
            if not tag_name:
                tag_name = f"AutoTag_{timestamp_str()}"
            chosen_tag = tag_name
        else:
            print("Available revision tags:")
            for i, t in enumerate(tags):
                tname = t.get("name") or t.get("id")
                print(f"  {i+1}) {tname}")
            sel = input("Enter number to select (or press Enter to cancel): ").strip()
            try:
                sel_idx = int(sel) - 1
                chosen_tag = tags[sel_idx].get("name") or tags[sel_idx].get("id")
            except Exception:
                print("Invalid selection; no tag chosen. Defaulting to latest if available.")
                if tags:
                    chosen_tag = tags[0].get("name") or tags[0].get("id")

    # Size estimation
    size_info = estimate_post_sync(token, prod_info, revision_file_path, logger)
    used_display = bytes_to_human(size_info["used"])
    alloc = size_info["alloc"]
    alloc_display = bytes_to_human(alloc) if alloc else "(unknown)"
    after_display = bytes_to_human(size_info["after"])
    pct_after = size_info["pct_after"]

    print("\nWorkspace Size Check for PROD:", prod_info.get("model_name"))
    print("-" * 50)
    print(f"Before Sync : {used_display} / {alloc_display}")
    print(f"After Sync  : {after_display} ({pct_after:.2%} of allocation)" if alloc else f"After Sync : {after_display} (allocation unknown)")

    proceed = True
    if alloc and pct_after > 0.95:
        print(f"{Colors.ERR}⚠ WARNING: Usage after sync will exceed 95% ({pct_after:.2%}).{Colors.RESET}")
        ans = input("Do you want to proceed with sync for this Prod? (y/n) [n]: ").strip().lower() or "n"
        proceed = ans == "y"
    else:
        print(f"{Colors.OK}Estimated usage after sync is acceptable ({pct_after:.2%}).{Colors.RESET}")

    return {
        "dev": dev_info["model_id"],
        "prod": prod_info["model_id"],
        "dev_name": dev_info.get("model_name"),
        "prod_name": prod_info.get("model_name"),
        "dev_ws": dev_info.get("workspace_id"),
        "prod_ws": prod_info.get("workspace_id"),
        "choice": choice,
        "tag_name": chosen_tag,
        "proceed": proceed,
        "size_info": size_info,
        "revision_file": revision_file_path
    }

# -------------------------
# Sync execution (parallel)
# -------------------------
def sync_revision_to_prod(task: Dict[str, Any], token: str, logger: logging.Logger) -> Dict[str, Any]:
    dev = task["dev"]
    prod = task["prod"]
    tag = task.get("tag_name")
    logger.info(f"Initiating sync: Dev={dev} ({task.get('dev_name')}) -> Prod={prod} ({task.get('prod_name')}) using tag '{tag}'")
    headers = {"Authorization": f"AnaplanAuthToken {token}", "Content-Type": "application/json"}
    # ALM endpoint for promote not publicized here; using representative placeholder
    url = f"{ANAPLAN_BASE}/2/0/models/{prod}/revisions/promote"
    payload = {"sourceModelId": dev, "revisionName": tag}
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
        if r.status_code in (200, 201, 202):
            logger.info(f"{Colors.OK}Sync request accepted for Prod={prod}{Colors.RESET}")
            return {"dev": dev, "prod": prod, "status": "started", "response": r.text}
        else:
            logger.error(f"{Colors.ERR}Sync failed for Prod={prod} - {r.status_code}: {r.text}{Colors.RESET}")
            return {"dev": dev, "prod": prod, "status": "failed", "response": r.text}
    except requests.RequestException as e:
        logger.error(f"{Colors.ERR}Sync request error for Prod={prod}: {e}{Colors.RESET}")
        return {"dev": dev, "prod": prod, "status": "error", "detail": str(e)}

def parallel_sync_executor(sync_tasks: List[Dict[str, Any]], token: str, logger: logging.Logger, max_workers: int = MAX_WORKERS) -> List[Dict[str, Any]]:
    to_run = [t for t in sync_tasks if t.get("proceed")]
    if not to_run:
        logger.info("No confirmed syncs. Skipping parallel sync phase.")
        return []
    logger.info(f"{Colors.BLUE}Starting parallel sync of {len(to_run)} task(s)...{Colors.RESET}")
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        # using executor.map to avoid explicit for-loop controlling each sync
        futures = ex.map(lambda t: sync_revision_to_prod(t, token, logger), to_run)
        for res in futures:
            results.append(res)
    logger.info("Parallel sync phase completed.")
    return results

# -------------------------
# Main flow
# -------------------------
def main():
    try:
        # 1) Archive logs and create new log
        archive_existing_logs(".")
        logpath, logger = create_new_log(LOGS_DIR)

        # 2) Ensure model history dir
        ensure_dir(MODEL_HISTORY_DIR)

        # 3) Prompt credentials & authenticate
        username, password = prompt_credentials()
        try:
            token = authenticate_anaplan(username, password, logger)
        except Exception as e:
            logger.error("Authentication failed. Exiting.")
            return

        # 4) Load config and parse model pairs
        try:
            config = load_config(CONFIG_FILE)
            export_action_name, pairs = parse_model_pairs(config)
            if not pairs:
                logger.error("No model pairs found in config. Exiting.")
                return
            logger.info(f"Found {len(pairs)} dev->prod pairs in config.")
        except Exception as e:
            logger.error(f"Failed reading config: {e}")
            return

        # 5) Discover model & workspace names for all model ids
        model_ids = []
        for p in pairs:
            model_ids.append(p["dev"])
            model_ids.append(p["prod"])
        model_ids = list(set(model_ids))
        logger.info("Discovering model & workspace names (this may take a few seconds)...")
        discovery = discover_model_and_workspace_names(token, model_ids, logger)

        # 6) Iterate each pair: download model history & interactively ask decisions
        sync_plan = []
        for idx, p in enumerate(pairs):
            dev = p["dev"]
            prod = p["prod"]
            dev_info = {
                "model_id": dev,
                "model_name": discovery.get(dev, {}).get("model_name"),
                "workspace_id": discovery.get(dev, {}).get("workspace_id"),
                "workspace_name": discovery.get(dev, {}).get("workspace_name")
            }
            prod_info = {
                "model_id": prod,
                "model_name": discovery.get(prod, {}).get("model_name"),
                "workspace_id": discovery.get(prod, {}).get("workspace_id"),
                "workspace_name": discovery.get(prod, {}).get("workspace_name")
            }

            # Download model history for dev using export_action_name
            mh_file = None
            try:
                logger.info(f"Downloading model history for Dev model {dev} using export '{export_action_name}'...")
                export_id = find_export_id_by_name(token, dev, export_action_name, logger)
                mh_file = run_export_and_download(token, dev, export_id, MODEL_HISTORY_DIR, logger)
            except Exception as e:
                logger.warning(f"Model history download failed for {dev}: {e}")
                # continue; revision_file may be None and size estimation will rely on API/0

            # Ask user for choices (collect for all pairs)
            try:
                details = ask_revision_choice_for_pair(idx, dev_info, prod_info, token, export_action_name, logger, mh_file)
                sync_plan.append(details)
            except Exception as e:
                logger.error(f"Interaction error for pair {dev}->{prod}: {e}")
                # add skip entry
                sync_plan.append({
                    "dev": dev, "prod": prod, "tag_name": None, "proceed": False, "revision_file": mh_file
                })

        # 7) Summary of user's plan before executing
        print("\n" + "=" * 72)
        print(f"{Colors.BLUE}Final Sync Plan Summary{Colors.RESET}")
        for i, s in enumerate(sync_plan, 1):
            status = "WILL SYNC" if s.get("proceed") else "SKIPPED"
            tag = s.get("tag_name") or "(none)"
            print(f"{i}. {s.get('dev_name') or s.get('dev')} -> {s.get('prod_name') or s.get('prod')} | Tag: {tag} | {status}")

        confirm_all = input("\nProceed to run syncs for items marked WILL SYNC? (y/n) [y]: ").strip().lower() or "y"
        if confirm_all != "y":
            logger.info("User cancelled final sync execution. Exiting.")
            return

        # 8) Execute syncs in parallel
        results = parallel_sync_executor(sync_plan, token, logger)

        # 9) Log summary
        logger.info("---- Sync Results ----")
        if not results:
            logger.info("No syncs executed.")
        else:
            for r in results:
                logger.info(json.dumps(r))
        logger.info("Script finished.")
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    except Exception as e:
        print(f"Fatal error: {e}")

if __name__ == "__main__":
    main()
