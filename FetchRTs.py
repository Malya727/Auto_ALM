import requests
import base64


# -------------------------------------------------------
# 1. AUTHENTICATION
# -------------------------------------------------------
def authenticate(username, password):
    url = "https://auth.anaplan.com/token/authenticate"

    auth_str = f"{username}:{password}"
    auth_b64 = base64.b64encode(auth_str.encode()).decode()

    headers = {
        "Authorization": f"Basic {auth_b64}",
        "Content-Type": "application/json"
    }

    r = requests.post(url, headers=headers)

    if r.status_code not in (200, 201):
        raise Exception(f"Authentication failed: {r.status_code} - {r.text}")

    return r.json()["tokenInfo"]["tokenValue"]


# -------------------------------------------------------
# 2. GET COMPATIBLE REVISION TAGS (DEV → PROD)
# -------------------------------------------------------
def get_compatible_revision_ids(token, prod_model_id, dev_model_id):
    """
    Returns a list of compatible revision tags as:
    [
        {"id": "<REV_ID>", "name": "<REV_NAME>"},
        ...
    ]
    """

    url = (
        f"https://api.anaplan.com/2/0/models/{prod_model_id}"
        f"/alm/syncableRevisions?sourceModelId={dev_model_id}"
    )

    headers = {
        "Authorization": f"AnaplanAuthToken {token}",
        "Content-Type": "application/json"
    }

    r = requests.get(url, headers=headers)

    if r.status_code not in (200, 201):
        raise Exception(f"Failed to fetch compatible revisions: {r.status_code} - {r.text}")

    revisions = r.json().get("revisions", [])

    if not revisions:
        print("⚠ No compatible revision tags found.")
        return []

    # Extract only id + name
    result = [{"id": rev["id"], "name": rev["name"]} for rev in revisions]

    return result


# -------------------------------------------------------
# 3. MAIN FUNCTION
# -------------------------------------------------------
def fetch_and_store_revision_ids(username, password, dev_ws, dev_model, prod_ws, prod_model):

    print("🔐 Authenticating...")
    token = authenticate(username, password)
    print("✔ Authenticated")

    print("📥 Fetching compatible revision tags from Dev → Prod...")
    revisions = get_compatible_revision_ids(token, prod_model, dev_model)

    print("\n🔽 Compatible Revisions Found:")
    for idx, r in enumerate(revisions, start=1):
        print(f"{idx}. {r['name']}  —  ID: {r['id']}")

    print("\n✔ IDs stored successfully in memory (Python list).")

    return revisions   # <-- You can store this or return to caller


# -------------------------------------------------------
# 4. EXAMPLE USAGE
# -------------------------------------------------------
if __name__ == "__main__":
    revs = fetch_and_store_revision_ids(
        username="your_email",
        password="your_password",
        dev_ws="DEV_WS_ID",
        dev_model="DEV_MODEL_ID",
        prod_ws="PROD_WS_ID",
        prod_model="PROD_MODEL_ID"
    )

    print("\nFINAL REVISION ID LIST RETURNED:")
    print(revs)
