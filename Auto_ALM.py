Prod={prod}")
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

import base64
import requests

def authenticate(username, password):
    try:
        # Create base64 token → username:password
        raw = f"{username}:{password}"
        encoded = base64.b64encode(raw.encode("utf-8")).decode("utf-8")
        headers = {
            "Authorization": f"Basic {encoded}",
            "Accept": "application/json"
        }

        # Call Anaplan Auth API
        url = "https://auth.anaplan.com/token/authenticate"
        response = requests.post(url, headers=headers)

        if response.status_code == 200:
            token = response.json()["tokenInfo"]["tokenValue"]
            print("✅ Authentication successful")
            return token
        else:
            print(f"❌ Authentication failed: {response.status_code}")
            print(response.text)
            return None

    except Exception as e:
        print("❌ Error during authentication:", str(e))
        return None
