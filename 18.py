import requests
import pwinput
import json
import logging
from datetime import datetime
from tabulate import tabulate
from requests.auth import HTTPBasicAuth

CONFIG_FILE = "config.json"
ANAPLAN_AUTH_URL = "https://auth.anaplan.com/token/authenticate"
ANAPLAN_API_BASE = "https://api.anaplan.com/2/0"

def timestamp():
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def setup_logger():
    logfile = f"Auto_ALM_{timestamp()}.log"
    logger = logging.getLogger("ALM")
    logger.setLevel(logging.DEBUG)
    if logger.handlers:
        logger.handlers.clear()
    fh = logging.FileHandler(logfile)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info(f"Log file: {logfile}")
    return logger

def authenticate(username, password):
    resp = requests.post(ANAPLAN_AUTH_URL, auth=HTTPBasicAuth(username, password), timeout=30)
    resp.raise_for_status()
    return resp.json()["tokenInfo"]["tokenValue"]

def find_workspace_for_model(token, model_id):
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    ws_resp = requests.get(f"{ANAPLAN_API_BASE}/workspaces", headers=headers, timeout=30)
    ws_resp.raise_for_status()
    for ws in ws_resp.json().get("workspaces", []):
        ws_id = ws.get("id")
        m_resp = requests.get(f"{ANAPLAN_API_BASE}/workspaces/{ws_id}/models", headers=headers, timeout=30)
        m_resp.raise_for_status()
        for m in m_resp.json().get("models", []):
            if m.get("id") == model_id:
                return ws_id
    raise RuntimeError(f"Workspace not found for model {model_id}")

def list_revision_tags(token, workspace_id, model_id):
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/models/{model_id}/revisions"
    headers = {"Authorization": f"AnaplanAuthToken {token}"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return [t["name"] for t in resp.json().get("revisions", [])]

def create_revision_tag(token, workspace_id, model_id, tag_name):
    url = f"{ANAPLAN_API_BASE}/workspaces/{workspace_id}/models/{model_id}/revisions"
    headers = {"Authorization": f"AnaplanAuthToken {token}", "Content-Type": "application/json"}
    payload = {"name": tag_name}
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return tag_name

def promote_revision_tag(token, dev_ws, dev_model, prod_ws, prod_model, tag_name):
    url = f"{ANAPLAN_API_BASE}/workspaces/{prod_ws}/models/{prod_model}/revisions/promote"
    headers = {"Authorization": f"AnaplanAuthToken {token}", "Content-Type": "application/json"}
    payload = {"sourceModelId": dev_model, "revisionName": tag_name}
    resp = requests.post(url, headers=headers, json=payload, timeout=60)
    resp.raise_for_status()
    return resp.status_code

def main():
    logger = setup_logger()

    # Load config
    with open(CONFIG_FILE) as f:
        config = json.load(f)
    export_name = config.get("Model Details", {}).get("export_action_name", "")
    model_pairs = config.get("Model Details", {}).get("model_ids", [])

    username = input("Enter Anaplan Username: ").strip()
    password = pwinput.pwinput("Enter password: ").strip()
    token = authenticate(username, password)
    logger.info(f"Authenticated user '{username}' successfully.")

    summary_data = []

    for idx, pair in enumerate(model_pairs, start=1):
        dev_id = pair.get("dev_model_id")
        prod_id = pair.get("prod_model_id")

        try:
            dev_ws = find_workspace_for_model(token, dev_id)
            prod_ws = find_workspace_for_model(token, prod_id)
            logger.info(f"Pair {idx}: DEV {dev_id} -> PROD {prod_id} | DEV WS {dev_ws} PROD WS {prod_ws}")
        except Exception as e:
            logger.error(f"Workspace fetch failed: {e}")
            summary_data.append([idx, dev_id, prod_id, "-", "-", "Workspace Not Found"])
            continue

        # RT options
        while True:
            try:
                tags = list_revision_tags(token, dev_ws, dev_id)
                print(f"\nRevision Tags for DEV model {dev_id}: {tags}")
                choice = input("RT Option: 1-Latest 2-Create 3-Select [1]: ").strip() or "1"

                if choice == "1":
                    tag = tags[-1] if tags else None
                    print("Using latest RT:", tag)
                elif choice == "2":
                    new_tag = input("Enter new RT name: ").strip()
                    tag = create_revision_tag(token, dev_ws, dev_id, new_tag)
                    print("Created RT:", tag)
                elif choice == "3":
                    for i, t in enumerate(tags, start=1):
                        print(f"{i}. {t}")
                    sel = int(input("Select RT number: ")) - 1
                    tag = tags[sel]
                    print("Selected RT:", tag)
                break
            except Exception as e:
                print(f"RT selection failed: {e}")
                retry = input("Retry? (y/n): ").strip().lower()
                if retry != "y":
                    tag = None
                    break

        if not tag:
            summary_data.append([idx, dev_id, prod_id, "-", "RT Selection Failed"])
            continue

        confirm = input(f"Promote RT '{tag}' from DEV -> PROD? (y/n) [y]: ").strip().lower() or "y"
        if confirm != "y":
            summary_data.append([idx, dev_id, prod_id, tag, "Skipped"])
            continue

        try:
            status_code = promote_revision_tag(token, dev_ws, dev_id, prod_ws, prod_id, tag)
            summary_data.append([idx, dev_id, prod_id, tag, f"Sync Initiated ({status_code})"])
            logger.info(f"Pair {idx} synced with RT '{tag}'.")
        except Exception as e:
            summary_data.append([idx, dev_id, prod_id, tag, f"Sync Failed: {e}"])
            logger.error(f"Pair {idx} sync failed: {e}")

    # Show summary table
    print("\n=== ALM Sync Summary ===")
    print(tabulate(summary_data, headers=["#", "DEV Model", "PROD Model", "RT", "Status"], tablefmt="grid"))

if __name__ == "__main__":
    main()
