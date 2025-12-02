#!/usr/bin/env python3
"""
Auto ALM sync script
- Function-based, simple names, minimal important comments.
- Interactive: collects input for each dev->prod pair, then runs confirmed syncs in parallel.
Dependencies: requests
"""

import os
import sys
import shutil
import json
import logging
import time
from datetime import datetime
from getpass import getpass
from typing import List, Dict, Tuple, Any
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# -------------------------
# Configuration / constants
# -------------------------
LOG_BACKUP_DIR = "Log_Backup"
LOGS_DIR = "Logs"
MODEL_HISTORY_DIR = "Model_History"
CONFIG_FILE = "config.json"
LOG_FILENAME_PREFIX = "Auto_ALM"
MODEL_HISTORY_PREFIX = "MODEL_HISTORY"
ANAPLAN_BASE = "https://api.anaplan.com"  # adjust if necessary
REQUEST_TIMEOUT = 30  # seconds
MAX_WORKERS = 4  # parallel workers for sync


# -------------------------
# Utility functions
# -------------------------
def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path)


def timestamp_str() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


# -------------------------
# Logging & file helpers
# -------------------------
def archive_existing_logs(main_dir: str = ".", backup_dir: str = LOG_BACKUP_DIR) -> List[str]:
    """
    Move any .log files present in main_dir into backup_dir.
    Return list of moved filenames (with dest path).
    """
    moved = []
    ensure_dir(backup_dir)
    # If backup_dir was just created, notify
    if not os.listdir(backup_dir):
        # If it's empty now and was just created, inform user
        print(f"Created folder: {backup_dir}")

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
    filepath = os.path.join(logs_dir, fname)

    logger = logging.getLogger("auto_alm")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    # File handler
    fh = logging.FileHandler(filepath)
    fh.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Console handler (info/warn)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(ch)

    logger.info(f"Log file created: {filepath}")
    return filepath, logger


# -------------------------
# Config & input
# -------------------------
def load_config(path: str = CONFIG_FILE) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_model_pairs(config: dict) -> Tuple[str, List[Dict[str, str]]]:
    """
    Return export_action_name and list of dicts {'dev':..., 'prod':...}
    """
    md = config.get("Model Details", {})
    export_action_name = md.get("export_action_name", "").strip()
    pairs = []
    for entry in md.get("model_ids", []):
        dev = entry.get("dev_model_id")
        prod = entry.get("prod_model_id")
        if dev and prod:
            pairs.append({"dev": dev, "prod": prod})
    return export_action_name, pairs


def prompt_credentials() -> Tuple[str, str]:
    user = input("Enter Anaplan username/email: ").strip()
    # getpass masks input (no echo). Not showing '*' by default.
    pwd = getpass("Enter Anaplan password (input hidden): ")
    return user, pwd


# -------------------------
# Anaplan API helpers
# -------------------------
def authenticate_anaplan(username: str, password: str, logger: logging.Logger) -> str:
    """
    Authenticate to Anaplan and return token string.
    This function uses Basic Auth to the Anaplan auth endpoint.
    Keep token in-memory only. Do not write to logs.
    """
    logger.info("Authenticating to Anaplan...")
    auth_url = f"{ANAPLAN_BASE}/2/0/authenticate"  # Anaplan's authenticate endpoint (v2)
    try:
        resp = requests.post(auth_url, auth=(username, password), timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            # response typically contains token in JSON or headers - adapt as needed
            # Many Anaplan flows return token in JSON: {"token": "..."}
            try:
                j = resp.json()
                token = j.get("token") or j.get("authToken") or j.get("authorizationToken")
            except Exception:
                token = None
            # fallback: header-based
            if not token:
                token = resp.headers.get("Authorization") or resp.headers.get("X-Auth-Token")
            if not token:
                logger.error("Authentication succeeded but token not found in response.")
                raise RuntimeError("Auth token missing")
            logger.info("Authentication successful.")
            return token
        else:
            logger.error(f"Authentication failed (status {resp.status_code}): {resp.text}")
            raise RuntimeError("Authentication failed")
    except requests.RequestException as e:
        logger.error(f"Authentication request failed: {e}")
        raise


def find_export_id_by_name(token: str, model_id: str, export_name: str, logger: logging.Logger) -> str:
    """
    Search exports of a model to find an export with given name.
    Returns export id or raises.
    """
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    url = f"{ANAPLAN_BASE}/2/0/models/{model_id}/exports"
    try:
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        # data format may be {'exports': [{'id':..., 'name':...}, ...]}
        exports = data.get("exports") or data.get("items") or []
        for e in exports:
            if e.get("name") == export_name:
                return e.get("id")
        raise RuntimeError(f"Export action '{export_name}' not found for model {model_id}")
    except requests.RequestException as e:
        logger.error(f"Failed to list exports for model {model_id}: {e}")
        raise


def run_export_and_download(token: str, model_id: str, export_id: str, out_dir: str, logger: logging.Logger) -> str:
    """
    Trigger export task and download result file into out_dir.
    Returns file path of downloaded model history.
    """
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    # 1) start export task
    start_url = f"{ANAPLAN_BASE}/2/0/models/{model_id}/exports/{export_id}/tasks"
    try:
        resp = requests.post(start_url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        task_info = resp.json()
        task_id = task_info.get("task", {}).get("id") or task_info.get("id")
        if not task_id:
            logger.error("Export task id not found after starting export.")
            raise RuntimeError("Export task id missing")
        # 2) poll for completion and get result endpoint
        task_url = f"{start_url}/{task_id}"
        logger.info("Waiting for export task to complete...")
        for _ in range(60):  # poll up to ~60*2 = 120s depending on sleep
            t_resp = requests.get(task_url, headers=headers, timeout=REQUEST_TIMEOUT)
            t_resp.raise_for_status()
            tdata = t_resp.json()
            status = (tdata.get("task") or tdata).get("status") if isinstance(tdata, dict) else None
            if status in ("COMPLETED", "Success", "completed", "success"):
                break
            time.sleep(2)
        # 3) fetch export result content
        # Many Anaplan exports provide a download link; some provide content at tasks endpoint
        download_url = f"{ANAPLAN_BASE}/2/0/models/{model_id}/exports/{export_id}/result"
        dl = requests.get(download_url, headers=headers, timeout=REQUEST_TIMEOUT, stream=True)
        dl.raise_for_status()
        ensure_dir(out_dir)
        fname = f"{model_id}_{timestamp_str()}.zip"
        fpath = os.path.join(out_dir, fname)
        with open(fpath, "wb") as fh:
            for chunk in dl.iter_content(chunk_size=8192):
                if chunk:
                    fh.write(chunk)
        logger.info(f"Downloaded model history: {fpath}")
        return fpath
    except requests.RequestException as e:
        logger.error(f"Export/download failed for model {model_id}: {e}")
        raise


# -------------------------
# Model history & revision helpers
# -------------------------
def download_model_history(token: str, dev_model_id: str, export_action_name: str, out_dir: str, logger: logging.Logger) -> str:
    """
    Find export by name for dev model, run it and download the result into out_dir.
    Returns path to downloaded file.
    """
    try:
        export_id = find_export_id_by_name(token, dev_model_id, export_action_name, logger)
        return run_export_and_download(token, dev_model_id, export_id, out_dir, logger)
    except Exception as e:
        logger.error(f"Could not download model history for {dev_model_id}: {e}")
        # Re-raise to let caller decide
        raise


def list_revision_tags(token: str, model_id: str, logger: logging.Logger) -> List[Dict[str, Any]]:
    """
    List revision tags for a model. Returns list of tags with name/id.
    """
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    url = f"{ANAPLAN_BASE}/2/0/models/{model_id}/modelRevisions"  # approximate endpoint
    try:
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        tags = data.get("revisions") or data.get("items") or []
        return tags
    except requests.RequestException:
        logger.warning("Could not fetch revision tags (API may differ); returning empty list.")
        return []


def create_revision_tag(token: str, model_id: str, tag_name: str, logger: logging.Logger) -> Dict[str, Any]:
    """
    Create a new revision tag on the dev model.
    Returns created tag info.
    """
    headers = {"Authorization": f"AnaplanAuthToken {token}", "Content-Type": "application/json"}
    url = f"{ANAPLAN_BASE}/2/0/models/{model_id}/modelRevisions"  # approximate endpoint
    payload = {"name": tag_name}
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logger.error(f"Failed to create revision tag '{tag_name}' on {model_id}: {e}")
        raise


# -------------------------
# Size estimation functions
# -------------------------
def get_workspace_usage(token: str, model_id: str, logger: logging.Logger) -> Tuple[int, int]:
    """
    Return (current_usage_bytes, allocation_bytes) for the workspace containing model_id.
    This attempts to find workspace id and then query usage. Implementation depends on API structure.
    """
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    # 1) find workspace id for model
    try:
        model_url = f"{ANAPLAN_BASE}/2/0/models/{model_id}"
        resp = requests.get(model_url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        mdata = resp.json().get("model") or resp.json()
        workspace_id = mdata.get("workspace", {}).get("id") or mdata.get("workspaceId") or mdata.get("workspaceId")
        if not workspace_id:
            logger.warning("Workspace id not found in model metadata; size estimation may not be accurate.")
            return 0, 0
        # 2) request workspace usage
        usage_url = f"{ANAPLAN_BASE}/2/0/workspaces/{workspace_id}/usage"
        uresp = requests.get(usage_url, headers=headers, timeout=REQUEST_TIMEOUT)
        uresp.raise_for_status()
        udata = uresp.json()
        used = udata.get("usedSpace") or udata.get("usedBytes") or udata.get("consumedBytes") or 0
        alloc = udata.get("allocatedSpace") or udata.get("allocatedBytes") or udata.get("allocationBytes") or 0
        return int(used), int(alloc)
    except requests.RequestException:
        logger.warning("Could not determine workspace usage via API; returning zeros.")
        return 0, 0


def estimate_post_sync_size(token: str, prod_model_id: str, revision_file_path: str, logger: logging.Logger) -> Dict[str, Any]:
    """
    Estimate size after applying RT using file size of downloaded revision as proxy.
    Returns dict with current, after, pct_after (0-1).
    """
    current_used, allocation = get_workspace_usage(token, prod_model_id, logger)
    revision_size = 0
    try:
        revision_size = os.path.getsize(revision_file_path)
    except Exception:
        logger.warning("Could not read revision file size; using 0 for estimation.")
    after = current_used + revision_size
    pct = (after / allocation) if allocation > 0 else 0.0
    return {"current": current_used, "allocation": allocation, "after": after, "pct_after": pct, "revision_size": revision_size}


# -------------------------
# Per-pair interactive prompts
# -------------------------
def ask_revision_choice_for_pair(idx: int, dev: str, prod: str, token: str, logger: logging.Logger, revision_file_path: str) -> Dict[str, Any]:
    """
    Interactively ask user for this pair how they want to pick RT.
    Returns dict with selection details.
    """
    print(f"\nPair #{idx + 1}: Dev={dev} -> Prod={prod}")
    logger.info(f"Processing pair #{idx + 1}: Dev={dev} -> Prod={prod}")

    # Ask user for choice
    print("Choose Revision Tag option:")
    print("  1) Use immediate available (latest) revision tag")
    print("  2) Create a new revision tag now")
    print("  3) List available revision tags and select one")
    choice = input("Enter choice (1/2/3): ").strip()

    selection = {"choice": choice, "tag_name": None}

    if choice == "1":
        # We'll interpret immediate/latest as 'latest' - we'll attempt to list revisions and pick newest
        tags = list_revision_tags(token, dev, logger)
        if tags:
            # pick the first/newest by whatever ordering API returns
            sel_tag = tags[0].get("name") or tags[0].get("id")
            selection["tag_name"] = sel_tag
            print(f"Selected immediate/latest tag: {sel_tag}")
        else:
            print("No tags found; you'll need to create one (option 2).")
            selection["choice"] = "2"
    elif choice == "2":
        tag_name = input("Enter name for new revision tag: ").strip()
        if not tag_name:
            tag_name = f"AutoTag_{timestamp_str()}"
            print(f"No name provided. Using '{tag_name}'")
        # create tag on dev
        try:
            created = create_revision_tag(token, dev, tag_name, logger)
            # try to extract created tag name/id
            name = created.get("name") or created.get("id") or tag_name
            selection["tag_name"] = name
            print(f"Created revision tag '{name}'.")
        except Exception:
            print("Failed to create revision tag via API; storing provided name and continuing.")
            selection["tag_name"] = tag_name
    elif choice == "3":
        tags = list_revision_tags(token, dev, logger)
        if not tags:
            print("No revision tags found.")
            # fall back to create
            selection["choice"] = "2"
            tag_name = input("Enter name for new revision tag: ").strip()
            selection["tag_name"] = tag_name or f"AutoTag_{timestamp_str()}"
        else:
            print("Available tags:")
            for i, t in enumerate(tags):
                tname = t.get("name") or t.get("id")
                print(f"  {i + 1}) {tname}")
            sel = input("Enter number to select: ").strip()
            try:
                sel_idx = int(sel) - 1
                chosen = tags[sel_idx]
                selection["tag_name"] = chosen.get("name") or chosen.get("id")
            except Exception:
                print("Invalid selection; skipping tag selection.")
    else:
        print("Invalid choice; defaulting to option 1 (immediate/latest).")
        selection["choice"] = "1"
        tags = list_revision_tags(token, dev, logger)
        selection["tag_name"] = tags[0].get("name") if tags else None

    # Size estimation and check
    size_info = estimate_post_sync_size(token, prod, revision_file_path, logger)
    print(f"Estimated Prod workspace usage BEFORE sync: {size_info['current']} bytes")
    if size_info["allocation"] > 0:
        pct_before = size_info["current"] / size_info["allocation"]
        print(f"Current usage: {pct_before:.2%} of allocation ({size_info['allocation']} bytes)")
    else:
        print("Allocation unknown; cannot compute percentages.")

    if size_info["pct_after"] > 0.95:
        # prompt user to continue or skip
        print(f"After applying this revision estimated usage would be {size_info['pct_after']:.2%} (>95%).")
        cont = input("Do you want to proceed with sync for this Prod? (y/n): ").strip().lower()
        proceed = cont == "y"
    else:
        print(f"After applying this revision estimated usage would be {size_info['pct_after']:.2%}. Proceeding.")
        proceed = True

    return {
        "dev": dev,
        "prod": prod,
        "choice": selection["choice"],
        "tag_name": selection["tag_name"],
        "proceed": proceed,
        "size_info": size_info,
        "revision_file": revision_file_path
    }


# -------------------------
# Sync execution
# -------------------------
def sync_revision_to_prod(task: Dict[str, Any], token: str, logger: logging.Logger) -> Dict[str, Any]:
    """
    Perform the actual sync promotion from dev->prod using chosen tag.
    Returns dict summarizing success/failure.
    NOTE: Endpoint and payload depend on Anaplan's ALM API. This is a representative implementation.
    """
    dev = task["dev"]
    prod = task["prod"]
    tag = task.get("tag_name")
    logger.info(f"Starting sync Dev={dev} -> Prod={prod} using tag='{tag}'")
    headers = {"Authorization": f"AnaplanAuthToken {token}", "Content-Type": "application/json"}
    try:
        # Representative endpoint: syncing revision from dev to prod may involve ALM endpoints.
        # The exact implementation will depend on your Anaplan ALM API availability.
        # As a generic approach we will call a placeholder endpoint:
        url = f"{ANAPLAN_BASE}/2/0/models/{prod}/revisions/promote"  # placeholder
        payload = {"sourceModelId": dev, "revisionName": tag}
        resp = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
        if resp.status_code in (200, 201, 202):
            logger.info(f"Sync initiated for Prod={prod}.")
            return {"dev": dev, "prod": prod, "status": "started", "detail": resp.text}
        else:
            logger.error(f"Sync failed for Prod={prod} (status {resp.status_code}): {resp.text}")
            return {"dev": dev, "prod": prod, "status": "failed", "detail": resp.text}
    except requests.RequestException as e:
        logger.error(f"Sync request failed for Prod={prod}: {e}")
        return {"dev": dev, "prod": prod, "status": "error", "detail": str(e)}


def parallel_sync_executor(sync_tasks: List[Dict[str, Any]], token: str, logger: logging.Logger, max_workers: int = MAX_WORKERS) -> List[Dict[str, Any]]:
    """
    Run sync_revision_to_prod for tasks in parallel using ThreadPoolExecutor and executor.map()
    Returns list of result dicts.
    """
    # Filter only tasks where proceed == True
    to_run = [t for t in sync_tasks if t.get("proceed")]
    if not to_run:
        logger.info("No tasks confirmed for syncing. Exiting sync phase.")
        return []

    logger.info(f"Starting parallel sync for {len(to_run)} tasks with {max_workers} workers...")
    ensure = []
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # executor.map will start functions concurrently and return results in order
        futures = executor.map(lambda t: sync_revision_to_prod(t, token, logger), to_run)
        # collect results
        for r in futures:
            results.append(r)

    logger.info("Parallel sync phase completed.")
    return results


# -------------------------
# Final logging & cleanup
# -------------------------
def log_summary_and_exit(results: List[Dict[str, Any]], logger: logging.Logger):
    logger.info("------ SUMMARY ------")
    if not results:
        logger.info("No syncs were executed.")
    else:
        for r in results:
            logger.info(json.dumps(r))
    logger.info("Script finished.")


# -------------------------
# Main flow
# -------------------------
def main():
    try:
        # 1) Archive existing logs
        archive_existing_logs(".")

        # 2) Create new log
        logpath, logger = create_new_log(LOGS_DIR)

        # 3) Ensure model history dir exists
        ensure_dir(MODEL_HISTORY_DIR)

        # 4) Prompt for credentials
        username, password = prompt_credentials()
        # 5) Authenticate
        try:
            token = authenticate_anaplan(username, password, logger)
        except Exception as e:
            logger.error("Authentication failed; exiting.")
            return

        # 6) Read config and parse pairs
        try:
            config = load_config(CONFIG_FILE)
            export_action_name, pairs = parse_model_pairs(config)
            if not pairs:
                logger.error("No model pairs found in config. Exiting.")
                return
            logger.info(f"Found {len(pairs)} model pairs in config.")
        except Exception as e:
            logger.error(f"Failed to read/parse config: {e}")
            return

        # 7) Iterate one-by-one to download histories and collect choices
        sync_plan = []
        for idx, pair in enumerate(pairs):
            dev = pair["dev"]
            prod = pair["prod"]
            try:
                logger.info(f"Downloading model history for dev model {dev} using export '{export_action_name}'...")
                mh_file = download_model_history(token, dev, export_action_name, MODEL_HISTORY_DIR, logger)
            except Exception:
                # download failed; still collect inputs, but revision file path set to None
                mh_file = None
                logger.warning(f"Model history download failed for dev={dev}. Continuing to interactive choices.")

            # interactively ask user for RT options and decide whether to proceed for this pair
            try:
                details = ask_revision_choice_for_pair(idx, dev, prod, token, logger, mh_file or "")
                sync_plan.append(details)
            except Exception as e:
                logger.error(f"Error during user interaction for pair #{idx + 1}: {e}")
                # add a skip entry
                sync_plan.append({"dev": dev, "prod": prod, "proceed": False, "tag_name": None, "size_info": {}, "revision_file": mh_file})

        # 8) At this point we have decisions for all pairs; prepare and run syncs in parallel
        results = parallel_sync_executor(sync_plan, token, logger)

        # 9) Log summary and exit
        log_summary_and_exit(results, logger)

    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    except Exception as e:
        print(f"Fatal error: {e}")


if __name__ == "__main__":
    main()
