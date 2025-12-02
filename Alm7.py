#!/usr/bin/env python3
"""
Auto ALM Sync - Full Script (Final)
- Username/password authentication
- Immediate Revision Tag creation
- Robust export task handling
- Workspace size estimation (MB/GB)
- Parallel sync of DEV->PROD pairs
- Table display per pair
- Logging & old log archive
Dependencies: requests, pwinput, tabulate
"""

import os, sys, json, time, shutil, logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional
import requests, pwinput
from requests.auth import HTTPBasicAuth

try:
    from tabulate import tabulate
    HAVE_TABULATE = True
except ImportError:
    HAVE_TABULATE = False

# -------------------
# Constants
# -------------------
CONFIG_FILE = "config.json"
LOG_BACKUP_DIR = "Log_Backup"
LOGS_DIR = "Logs"
MODEL_HISTORY_DIR = "Model_History"
LOG_FILENAME_PREFIX = "Auto_ALM"
ANAPLAN_AUTH_URL = "https://auth.anaplan.com/token/authenticate"
ANAPLAN_API_BASE = "https://api.anaplan.com/2/0"
REQUEST_TIMEOUT = 30
MAX_WORKERS = 6

# -------------------
# Utilities
# -------------------
def timestamp_str() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def bytes_to_human(nbytes: int) -> str:
    gb = 1024 ** 3
    mb = 1024 ** 2
    if nbytes >= gb:
        return f"{nbytes/gb:,.2f} GB"
    else:
        return f"{nbytes/mb:,.2f} MB"

# -------------------
# Logging & archive
# -------------------
def archive_existing_logs(main_dir="."):
    ensure_dir(LOG_BACKUP_DIR)
    if not os.listdir(LOG_BACKUP_DIR):
        print(f"Created folder: {LOG_BACKUP_DIR}")
    for entry in os.listdir(main_dir):
        if entry.lower().endswith(".log") and os.path.isfile(entry):
            shutil.move(entry, os.path.join(LOG_BACKUP_DIR, entry))
            print(f"Moved log file '{entry}' -> '{LOG_BACKUP_DIR}'")

def create_new_log() -> logging.Logger:
    ensure_dir(LOGS_DIR)
    fname = f"{LOG_FILENAME_PREFIX}_{timestamp_str()}.log"
    filepath = os.path.join(LOGS_DIR, fname)
    logger = logging.getLogger("auto_alm")
    logger.setLevel(logging.DEBUG)
    if logger.handlers:
        logger.handlers.clear()
    fh = logging.FileHandler(filepath)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info(f"Log file created: {filepath}")
    return logger

# -------------------
# Config
# -------------------
def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)

def parse_model_pairs(config: dict) -> (str, List[Dict[str,str]]):
    md = config.get("Model Details", {})
    export_action_name = md.get("export_action_name", "").strip()
    pairs = [{"dev": m.get("dev_model_id"), "prod": m.get("prod_model_id")} for m in md.get("model_ids",[])]
    return export_action_name, pairs

# -------------------
# Authentication
# -------------------
def authentication(username: str, password: str) -> str:
    resp = requests.post(ANAPLAN_AUTH_URL, auth=HTTPBasicAuth(username, password), timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()["tokenInfo"]["tokenValue"]

def prompt_credentials_and_auth(logger: logging.Logger) -> str:
    username = input("Enter your Anaplan Username/Email: ").strip()
    password = pwinput.pwinput("Enter your Anaplan password: ")
    try:
        token = authentication(username, password)
        logger.info(f"User '{username}' logged in successfully")
        return token
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        raise

# -------------------
# Model discovery
# -------------------
def get_all_workspaces(token: str, logger: logging.Logger):
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    url = f"{ANAPLAN_API_BASE}/workspaces"
    try:
        r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json().get("workspaces") or r.json().get("items") or []
    except Exception as e:
        logger.warning(f"Could not list workspaces: {e}")
        return []

def get_models_in_workspace(token: str, workspace_id: str, logger: logging.Logger):
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/models"
    try:
        r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json().get("models") or r.json().get("items") or []
    except Exception as e:
        logger.warning(f"Could not list models in workspace {workspace_id}: {e}")
        return []

def discover_model_metadata(token: str, model_ids: List[str], logger: logging.Logger):
    result = {}
    workspaces = get_all_workspaces(token, logger)
    for ws in workspaces:
        ws_id = ws.get("id") or ws.get("workspaceId")
        ws_name = ws.get("name")
        if not ws_id: continue
        models = get_models_in_workspace(token, ws_id, logger)
        for m in models:
            mid = m.get("id") or m.get("modelId")
            if mid in model_ids:
                result[mid] = {"model_name": m.get("name") or "(unknown)", "workspace_id": ws_id, "workspace_name": ws_name or "(unknown)"}
    for mid in model_ids:
        if mid not in result:
            result[mid] = {"model_name": "(unknown)", "workspace_id": None, "workspace_name": "(unknown)"}
    return result

# -------------------
# Model History Export
# -------------------
def find_export_id_by_name(token: str, model_id: str, export_name: str, logger: logging.Logger):
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    url = f"{ANAPLAN_API_BASE}/models/{model_id}/exports"
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    for e in r.json().get("exports") or []:
        if e.get("name") == export_name:
            return e.get("id")
    raise RuntimeError(f"Export '{export_name}' not found for model {model_id}")

def run_export_and_download(token: str, workspace_id: str, model_id: str, export_id: str, out_dir: str, logger: logging.Logger):
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    start_url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/models/{model_id}/exports/{export_id}/tasks"
    
    r = requests.post(start_url, headers=headers, json={"localeName": "en_US"}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    
    # Robust task ID extraction
    task_data = r.json()
    task_id = None
    if isinstance(task_data, dict):
        if "task" in task_data and isinstance(task_data["task"], dict):
            task_id = task_data["task"].get("id") or task_data["task"].get("taskId")
        if not task_id:
            task_id = task_data.get("id") or task_data.get("taskId")
    
    if not task_id:
        logger.error(f"Failed to find export task ID in response: {task_data}")
        raise RuntimeError("Could not start export task: task ID missing in response")
    
    logger.info(f"Export task started. Task ID: {task_id}")
    
    # Poll for completion
    task_url = f"{start_url}/{task_id}"
    for _ in range(60):
        time.sleep(2)
        t_resp = requests.get(task_url, headers=headers, timeout=REQUEST_TIMEOUT)
        t_resp.raise_for_status()
        tdata = t_resp.json()
        status = None
        if "task" in tdata and isinstance(tdata["task"], dict):
            status = tdata["task"].get("status")
        if not status:
            status = tdata.get("status")
        
        # Dict issue fix: convert to string
        status_str = str(status).lower() if status else ""
        if status_str in ("completed", "success"):
            break
    else:
        raise RuntimeError("Export task did not complete in expected time")
    
    # Download result
    download_url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/models/{model_id}/exports/{export_id}/result"
    dl = requests.get(download_url, headers=headers, stream=True, timeout=REQUEST_TIMEOUT)
    dl.raise_for_status()
    
    ensure_dir(out_dir)
    fname = f"{model_id}_{timestamp_str()}.zip"
    fpath = os.path.join(out_dir, fname)
    with open(fpath, "wb") as fh:
        for chunk in dl.iter_content(8192):
            if chunk:
                fh.write(chunk)
    
    logger.info(f"Downloaded model history to: {fpath}")
    return fpath

# -------------------
# Revision Tag
# -------------------
def create_revision_tag(token: str, model_id: str, tag_name: str, logger: logging.Logger, workspace_id: str):
    """Create Revision Tag immediately on DEV model."""
    if not workspace_id:
        raise ValueError("Workspace ID required to create a revision tag")
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/models/{model_id}/revisions"
    headers = {"Authorization": f"AnaplanAuthToken {token}", "Content-Type": "application/json"}
    payload = {"name": tag_name}
    r = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
    if r.status_code in (201, 202):
        logger.info(f"Revision Tag '{tag_name}' created on DEV model {model_id}")
        print(f"✅ Revision Tag '{tag_name}' created on DEV model.")
        return r.json()
    else:
        logger.error(f"Failed to create Revision Tag '{tag_name}' (status {r.status_code}): {r.text}")
        raise RuntimeError(f"Failed to create Revision Tag '{tag_name}'")

def list_revision_tags(token: str, model_id: str, logger: logging.Logger, workspace_id: Optional[str]=None):
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    url=f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/models/{model_id}/revisions" if workspace_id else f"{ANAPLAN_API_BASE}/models/{model_id}/revisions"
    try:
        r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        j=r.json()
        for key in ("revisions","items","modelRevisions"):
            if key in j and isinstance(j[key], list):
                return j[key]
    except:
        return []
    return []

# -------------------
# Workspace size
# -------------------
def get_workspace_usage(token: str, workspace_id: str, logger: logging.Logger):
    if not workspace_id: return 0,0
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    url=f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/usage"
    try:
        r=requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        d=r.json()
        used=d.get("used") or d.get("usedBytes") or 0
        alloc=d.get("allocated") or d.get("allocatedBytes") or 0
        return int(used), int(alloc)
    except: return 0,0

def estimate_post_sync_size(token: str, prod_info: dict, revision_file_path: str, logger):
    ws=prod_info.get("workspace_id")
    used, alloc=get_workspace_usage(token, ws, logger)
    rev_size=0
    if revision_file_path and os.path.exists(revision_file_path):
        rev_size=os.path.getsize(revision_file_path)
    after=used+rev_size
    pct_after=(after/alloc) if alloc else 0
    return {"current": used,"alloc":alloc,"revision_size":rev_size,"after":after,"pct_after":pct_after}

# -------------------
# Table display
# -------------------
def pretty_table(dev_info:dict, prod_info:dict) -> str:
    headers=["Info","DEV","PROD"]
    rows=[["Model ID", dev_info.get("model_id"), prod_info.get("model_id")],
          ["Model Name", dev_info.get("model_name"), prod_info.get("model_name")],
          ["Workspace", dev_info.get("workspace_name"), prod_info.get("workspace_name")]]
    if HAVE_TABULATE:
        return tabulate(rows, headers=headers, tablefmt="pretty")
    else:
        col1w=18; colw=36; sep="+"+"-"*(col1w+2)+"+"+"-"*(colw+2)+"+"+"-"*(colw+2)+ "+"
        lines=[sep,"| {:<{}} | {:<{}} | {:<{}} |".format("Info",col1w,"DEV",colw,"PROD",colw), sep]
        for r in rows: lines.append("| {:<{}} | {:<{}} | {:<{}} |".format(r[0],col1w,str(r[1])[:colw],colw,str(r[2])[:colw],colw))
        lines.append(sep)
        return "\n".join(lines)

# -------------------
# Per-pair interaction
# -------------------
def ask_revision_choice_for_pair(idx:int, dev_info:dict, prod_info:dict, token:str, export_action_name:str, logger, revision_file_path:str):
    print("\n"+"="*64)
    print(f"Pair #{idx}: {dev_info.get('model_name')} -> {prod_info.get('model_name')}")
    print(pretty_table(dev_info, prod_info))
    print("\nRevision Tag options:\n 1) Use latest\n 2) Create new\n 3) List & select")
    choice = input("Enter choice (1/2/3) [1]: ").strip() or "1"
    selected_tag = None
    if choice=="1":
        tags=list_revision_tags(token, dev_info["model_id"], logger, workspace_id=dev_info.get("workspace_id"))
        selected_tag = tags[0].get("name") if tags else f"AutoTag_{timestamp_str()}"
    elif choice=="2":
        tag_name = input("Enter new tag name (blank=auto): ").strip() or f"AutoTag_{timestamp_str()}"
        created=create_revision_tag(token, dev_info["model_id"], tag_name, logger, workspace_id=dev_info.get("workspace_id"))
        selected_tag = created.get("name") or created.get("id") or tag_name
    elif choice=="3":
        tags=list_revision_tags(token, dev_info["model_id"], logger, workspace_id=dev_info.get("workspace_id"))
        if tags:
            for i,t in enumerate(tags): print(f"  {i+1}) {t.get('name')}")
            sel=input("Choose number [1]: ").strip() or "1"
            try: selected_tag=tags[int(sel)-1].get("name")
            except: selected_tag=tags[0].get("name")
        else:
            selected_tag=f"AutoTag_{timestamp_str()}"
    size_info=estimate_post_sync_size(token, prod_info, revision_file_path, logger)
    proceed=True
    print(f"\nWorkspace size before sync: {bytes_to_human(size_info['current'])} / {bytes_to_human(size_info['alloc']) if size_info['alloc'] else '(unknown)'}")
    print(f"Estimated after sync: {bytes_to_human(size_info['after'])} ({size_info['pct_after']:.2%})")
    if size_info['alloc'] and size_info['pct_after']>0.95:
        ans=input("Usage >95%. Proceed? (y/n) [n]: ").strip().lower() or "n"
        proceed=ans=="y"
    return {"dev":dev_info["model_id"],"prod":prod_info["model_id"],"tag":selected_tag,"proceed":proceed,"revision_file":revision_file_path,"dev_ws":dev_info.get("workspace_id"),"prod_ws":prod_info.get("workspace_id")}

# -------------------
# Sync execution
# -------------------
def promote_revision_to_prod(task, token, logger):
    dev=task["dev"]; prod=task["prod"]; prod_ws=task.get("prod_ws"); tag=task.get("tag")
    headers={"Authorization": f"AnaplanAuthToken {token}","Content-Type":"application/json"}
    url=f"{ANAPLAN_API_BASE}/workspaces/{prod_ws}/models/{prod}/revisions/promote"
    payload={"sourceModelId": dev,"revisionName": tag}
    try:
        r=requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
        if r.status_code in (200,201,202): logger.info(f"Promote request accepted for Prod={prod} (tag={tag})")
        else: logger.error(f"Promote failed Prod={prod}: {r.status_code} {r.text}")
        return {"dev":dev,"prod":prod,"tag":tag,"status":r.status_code}
    except Exception as e:
        logger.error(f"Exception promoting {prod}: {e}")
        return {"dev":dev,"prod":prod,"tag":tag,"status":"Exception"}

def parallel_sync_executor(sync_tasks: List[dict], token:str, logger: logging.Logger):
    to_run=[t for t in sync_tasks if t["proceed"]]
    if not to_run: return
    logger.info(f"\nStarting parallel promotion for {len(to_run)} pairs...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
        futures=[exe.submit(promote_revision_to_prod,t,token,logger) for t in to_run]
        for f in futures: f.result()

# -------------------
# Main
# -------------------
def main():
    archive_existing_logs()
    logger=create_new_log()
    ensure_dir(MODEL_HISTORY_DIR)
    config=load_config(CONFIG_FILE)
    export_action_name, pairs=parse_model_pairs(config)
    logger.info(f"Export action: {export_action_name}")
    token=prompt_credentials_and_auth(logger)

    # Discover model metadata
    all_model_ids=[p["dev"] for p in pairs]+[p["prod"] for p in pairs]
    meta=discover_model_metadata(token, all_model_ids, logger)

    # Per pair handling
    all_tasks=[]
    for idx,p in enumerate(pairs,1):
        dev_info={"model_id":p["dev"], **meta[p["dev"]]}
        prod_info={"model_id":p["prod"], **meta[p["prod"]]}
        # Download Model History
        try:
            export_id=find_export_id_by_name(token, dev_info["model_id"], export_action_name, logger)
            rev_file=run_export_and_download(token, dev_info["workspace_id"], dev_info["model_id"], export_id, MODEL_HISTORY_DIR, logger)
        except Exception as e:
            logger.error(f"Failed to download model history for DEV={dev_info['model_id']}: {e}")
            rev_file=None
        task=ask_revision_choice_for_pair(idx, dev_info, prod_info, token, export_action_name, logger, rev_file)
        all_tasks.append(task)

    # Parallel Sync
    parallel_sync_executor(all_tasks, token, logger)
    logger.info("ALM sync process completed.")

if __name__=="__main__":
    main()
