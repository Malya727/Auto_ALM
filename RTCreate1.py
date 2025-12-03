import base64
import requests
import json

# -----------------------------------------
# VARIABLES (You can edit these)
# -----------------------------------------
username = "your_email@domain.com"
password = "your_password"

workspace_id = "YOUR_WORKSPACE_ID"
model_id = "YOUR_MODEL_ID"

revision_name = "My Automated Revision Tag"
revision_desc = "Created via Python script using ALM API"


# -----------------------------------------
# STEP 1: GENERATE AUTH TOKEN (Basic Auth)
# -----------------------------------------
def get_auth_token(username, password):
    auth_url = "https://auth.anaplan.com/token/authenticate"

    # Format: username:password → Base64 encode
    auth_str = f"{username}:{password}"
    auth_bytes = auth_str.encode("utf-8")
    auth_encoded = base64.b64encode(auth_bytes).decode("utf-8")

    headers = {
        "Authorization": f"Basic {auth_encoded}"
    }

    response = requests.post(auth_url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Auth Failed! Status: {response.status_code}, Response: {response.text}")

    token = response.json()['tokenInfo']['tokenValue']
    print("✔ Authentication successful")

    return token


# -----------------------------------------
# STEP 2: CREATE REVISION TAG
# -----------------------------------------
def create_revision_tag(model_id, token, revision_name, revision_desc):
    url = f"https://api.anaplan.com/2/0/models/{model_id}/alm/revisions"

    headers = {
        "Authorization": f"AnaplanAuthToken {token}",
        "Content-Type": "application/json"
    }

    body = {
        "name": revision_name,
        "description": revision_desc
    }

    response = requests.post(url, headers=headers, data=json.dumps(body))

    print("\n--- API RESPONSE ---")
    print("Status Code:", response.status_code)
    print("Response Body:", response.text)

    if response.status_code == 201:
        print("✔ Revision Tag Created Successfully!")
    else:
        print("❌ Failed to Create Revision Tag")


# -----------------------------------------
# MAIN EXECUTION
# -----------------------------------------
try:
    token = get_auth_token(username, password)
    create_revision_tag(model_id, token, revision_name, revision_desc)
except Exception as e:
    print("Error:", str(e))
