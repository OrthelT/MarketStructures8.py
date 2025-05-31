---------------------------------------------
ESI Structure Market Tools for Eve Online
---------------------------------------------
Tool for coaxing data out of Eve Online player-owned markets.

What it does

- Authenticate a character through Eve's SSO
- Gently retrieves all market orders for a private player market from the ESI market-structures endpoint with robust error handling.
- Retrieve 30-day market history history for a list of Type_Ids stored as a .csv file
- Process data into summary statistics
- Exports data as a .csv

## Installation

Instructions on how to install, configure, and use the project.

1) Register through the Eve developer portal. https://developers.eveonline.com/
- Create an application with the following scopes: esi-markets.structure_markets.v1
- give it a callback URL: (example -- http://localhost:8000/callback)
- Copy the CLIENT_ID, SECRET_KEY, and Callback URL


1) Create a .env file to store your credentials:

CLIENT_ID = '...'

SECRET_KEY = '..'

2) Configure variables in the main file for the structure you want and edit the type_ids .csv list to your liking.

3) Ensure the following file structure:

##Project Folder
.env file
.gitignore
file_cleanup.py
Market_Structures8.py
requirements.txt

#subfolders
 - data
    type_ids.csv #full list of items for market history requests
    type_ids2.csv #shorter list to use for debugging
- output
    - archive
    - error_logs
    - latest
####

## Usage

Run: Code will ask you to run in testing mode.
Good idea for first time through to make sure it's all working.

Pulls market orders for the chosen structure and regional history for the type ids on your list.

Outputs
- MarketStats (summary stats)
- MarketOrders (all)
- MarketHistory (all orders your your type-ids over the last 30 days.
- error_log so you can see if anything went wrong.


Project Link: [https://github.com/yourusername/yourproject](https://github.com/yourusername/yourproject)
