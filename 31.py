I want to automate the Anaplan ALM process using Python, and here are my requirements:

Create a detailed log for each step performed.
When starting a new run, move previous logs to a backup folder.
Log files should include "AUTO_ALM" along with the date and time to ensure unique filenames.
Authenticate into Anaplan using Basic Authentication and obtain a token for further API calls.
Use a config.json file containing all dev and prod model IDs for performing ALM from Dev to Prod.
After reading the config.json file, iterate over all dev-prod combinations, and display their details in a table before proceeding.
The table should show model ID, model name, workspace ID, workspace name, current workspace size, and total workspace limit.
After displaying these details, prompt the user to choose whether to create a new RT in Dev and sync it to Prod, sync the latest available RT in Dev to Prod, or view available RTs in Dev and select one to sync.
Once an option is selected, perform the respective task. Before syncing the RT, analyze the Prod model size and show it to the user, then get their confirmation to proceed with the sync.
Store all these details in a list, and only sync to Prod models where the user has given confirmation after the size analysis.
