import requests
import os
import json
import pwinput
from requests.auth import HTTPBasicAuth

CONFIG_PATH = "config.json"
CONFIG_OBJ = open("config.json")
CONFIG_JSON = json.load(CONFIG_OBJ)

#Authenticate user with username and password
def authentication(username, password):
    url = "https://auth.anaplan.com/token/authenticate"
    response = (requests.post(url, auth=HTTPBasicAuth(username, password))).json()

    auth_token = response["tokenInfo"]["tokenValue"]
    return auth_token

def log():
    pass

def storeHistory(auth_token, dev_num):
    #model metadata
    #print(CONFIG_JSON)
    
    workspace_id = CONFIG_JSON["model_type"]["dev"][(f"model_{dev_num}")]["workspace_id"]
    model_id = CONFIG_JSON["model_type"]["dev"][(f"model_{dev_num}")]["model_id"]
    action_id = CONFIG_JSON["model_type"]["dev"][(f"model_{dev_num}")]["export_id"]

    print(workspace_id, model_id, action_id)

    url = (f'https://api.anaplan.com/2/0/workspaces/{workspace_id}/models/{model_id}/exports/{action_id}/tasks/')
    
    headers = {
        "authorization": f"AnaplanAuthToken {auth_token}",
        "Content-Type": "application/json"
    }

    data = json.dumps({"localeName": "en_US"})

    export = requests.post(url, headers=headers, data=data)

    print(export.status_code)

    #pass

    folder_name = "Model History Exports"

    print(f"Model History Audits will be stored in the folder: '{folder_name}'")
    try:
        os.mkdir(folder_name)
    except FileExistsError:
        print("")
    except PermissionError:
        print(f"Permission denied: Unable to create '{folder_name}'")
    except Exception as error:
        print(f"An error occured: {error}")

def get_model_status(auth_token, model_type, model_num):
    model_id = CONFIG_JSON["model_type"][(f"{model_type}")][(f"model_{model_num}")]["model_id"]
    print(model_id)
    url = (f"https://api.anaplan.com/2/0/models/{model_id}/?modelDetails=true")

     
    headers = {
        "Authorization":"AnaplanAuthToken " + auth_token,
        "Content-Type":"application/json"
    }

    response = requests.get(url, headers=headers).json()

    print(response)




def main():
    username = input("Enter your Anaplan Username/Email ID: ")
    password = pwinput.pwinput("Enter your Anaplan password: ")
    
    try: 
        auth_token = authentication(username, password)
    except:
        print("Invalid Username/Password !")
        print("Please try again. You have 4 attempts remaining.")
    else:
        print(f"User '{username}' logged in Successfully")

    dev_num = input("Enter dev model number for history export")
    storeHistory(auth_token, dev_num)
    #get_model_status(auth_token,"dev",dev_num)

if __name__=="__main__":
    main()
#History exports

#Workspace size feasibility

#model selection
