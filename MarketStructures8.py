import os


import requests
import webbrowser
import time
import csv
import pandas as pd
import json
import numpy as np
from requests import ReadTimeout
from datetime import datetime
from prompt_toolkit.styles import Style
from requests_oauthlib import OAuth2Session
from dotenv import load_dotenv
from file_cleanup import rename_move_and_archive_csv

load_dotenv()
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'


CLIENT_ID = os.getenv('CLIENT_ID')
SECRET_KEY = os.getenv('SECRET_KEY')
REDIRECT_URI = 'http://localhost:8000/callback'
AUTHORIZATION_URL = 'https://login.eveonline.com/v2/oauth/authorize'
TOKEN_URL = 'https://login.eveonline.com/v2/oauth/token'
MARKET_STRUCTURE_URL = 'https://esi.evetech.net/latest/markets/structures/1035466617946/?page='
SCOPE = ['esi-markets.structure_markets.v1']
TOKEN_FILE = 'token.json'


def save_token(token):
    # Save the OAuth token including refresh token to a file.
    with open(TOKEN_FILE, 'w') as f:
        json.dump(token, f)

def load_token():
    # Load the OAuth token from a file, if it exists.
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'r') as f:
            return json.load(f)
    return None

def get_oauth_session(token=None):
    # Get an OAuth session, refreshing the token if necessary.
    extra = {'client_id': CLIENT_ID, 'client_secret': SECRET_KEY}
    if token:
        return OAuth2Session(CLIENT_ID, token=token, auto_refresh_url=TOKEN_URL, auto_refresh_kwargs=extra, token_updater=save_token)
    else:
        return OAuth2Session(CLIENT_ID, redirect_uri=REDIRECT_URI, scope=SCOPE)

# Step 1: Redirect user to the EVE Online login page to get the authorization code
def get_authorization_code():
    # Step 1: Redirect user to the EVE Online login page to get the authorization code.
    oauth = get_oauth_session()
    authorization_url, state = oauth.authorization_url(AUTHORIZATION_URL)
    print(f"Please go to this URL and authorize access: {authorization_url}")
    webbrowser.open(authorization_url)
    redirect_response = input('Paste the full redirect URL here: ')
    token = oauth.fetch_token(TOKEN_URL, authorization_response=redirect_response, client_secret=SECRET_KEY)
    save_token(token)
    return token

def get_token():
    # Retrieve a token, refreshing it using the refresh token if available.
    token = load_token()

    if token:
        oauth = get_oauth_session(token)
        if oauth.token['expires_at'] < time.time():
            print("Token expired, refreshing token...")
            token = oauth.refresh_token(TOKEN_URL, client_id=CLIENT_ID, client_secret=SECRET_KEY)
            save_token(token)
        return token
    else:
        return get_authorization_code()

# Step 3: Fetch market orders from the structure, handling pagination
def fetch_market_orders(test_mode):

    token = get_token()

    headers = {
        'Authorization': f'Bearer {token["access_token"]}',
        'Content-Type': 'application/json',
    }

    page = 1
    max_pages = 1
    tries = 0
    total_tries = 0
    error_count = 0
    total_pages = 0

    all_orders = []
    errorlog = {}

    while page <= max_pages:
        response = requests.get(MARKET_STRUCTURE_URL + str(page), headers=headers)
        print(f"Fetching page {page}, status code: {response.status_code}")  # Track status

        if 'X-Pages' in response.headers:
            max_pages = int(response.headers['X-Pages'])
        elif response.status_code == 200:
            max_pages = 1

        if test_mode == True:
            max_pages = 5

        errorsleft = int(response.headers.get('X-ESI-Error-Limit-Remain', 0))
        errorreset = int(response.headers.get('X-ESI-Error-Limit-Reset', 0))

        if errorsleft == 0:
            break
        elif errorsleft < 10:
            print(f'WARNING: Errors remaining: {errorsleft}. Error limit reset: {errorreset} seconds.')

        if response.status_code != 200:
            error_code = response.status_code
            print(f"Error fetching data from page {page}. status code: {error_code}. tries: {tries} Retrying in 3 seconds...")
            print(os.strerror(error_code))

            error_count += 1

            if error_code not in errorlog:
                errorlog[error_code] = []  # Initialize the list if it doesn't exist
            errorlog[error_code].append(page)

            if tries < 5:
                tries += 1
                time.sleep(3)
                continue
            else:
                print(f'Reached the 5th try and giving up on page {page}.')
                break

        total_tries += tries
        tries = 0

        try:
            orders = response.json()
        except ValueError:
            print(f"Error decoding JSON response from page {page}. Skipping to next page.")
            continue

        if not orders:
            break

        all_orders.extend(orders)

        print(f"Orders fetched from page {page}/{max_pages}: {len(orders)}. Total Orders fetched: {len(all_orders)}.")

        total_pages += 1
        page += 1

        print('-------------------------')
        print(f"Now fetching page {page}...")
        time.sleep(1)

    print(f"Retrieval complete. Fetched {total_pages}. Total orders: {len(all_orders)}")
    print(f"Received {error_count} errors. After {total_tries} total tries.")
    print("Returning all orders....")

    return all_orders, errorlog

# Step 4: Save the market orders into a CSV file
def save_to_csv(orders):
    filename = f"output/4Hmarketorders_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    fields = ['order_id', 'price', 'volume_remain', 'volume_total', 'is_buy_order', 'issued', 'type_id', 'range']

    with open(filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        for order in orders:
            writer.writerow({
                'order_id': order.get('order_id'),
                'price': order.get('price'),
                'volume_remain': order.get('volume_remain'),
                'volume_total': order.get('volume_total'),
                'is_buy_order': order.get('is_buy_order'),
                'issued': order.get('issued'),
                'type_id': order.get('type_id'),
                'range': order.get('range')
            })

    print(f"Market orders saved to {filename}")

# Step 4a: Save the error log into a CSV file
def save_error_log_to_csv(errorlog):
    filename = f"output/4Hmarketorders_errorlog_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"

    # Open CSV file in write mode
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)

        # Write the header
        writer.writerow(['Error Code', 'Pages'])

        # Write each error code and the pages it occurred on
        for error_code, pages in errorlog.items():
            pages_str = ', '.join(map(str, pages))  # Convert list of pages to a comma-separated string
            writer.writerow([error_code, pages_str])

    print(f"Error log saved to {filename}")

#Get the type ids to use for market history
def get_type_ids(datafile):

    watchlist = pd.read_csv(datafile,
                            usecols=[0],
                            names=['type_id'],skiprows=1
                            )
    type_ids = watchlist['type_id'].tolist()
    return type_ids

#update market history
def fetch_market_history(type_id_list):
    TIMEOUT = 10
    market_history_url = 'https://esi.evetech.net/latest/markets/10000003/history/?datasource=tranquility&type_id='

    headers = {
        'Content-Type': 'application/json',
    }
    all_history = []
    page = 1
    max_pages = 1
    errorcount = 0
    tries = 0
    successful_returns = 0

    print("Updating market history...")

    #Iterate over type_ids to fetch market history for 4-HWWF
    for type_id in range(len(type_id_list)):
        while page <= max_pages:
            item = type_id_list[type_id]

            try:
                response = requests.get(market_history_url + str(item), headers=headers, timeout=TIMEOUT)
                print(f"Fetching type_id: {item}, status code: {response.status_code}")  # Add this line to track status
            except ReadTimeout:
                print(f"Request timed out for page {page}, item {item}. Retrying...")
                if tries < 5:
                    tries += 1
                    time.sleep(3)  # Wait before retrying
                    continue

            page += 1
            tries = 0

            if 'X-Pages' in response.headers:
                max_pages = int(response.headers['X-Pages'])
            else:
                max_pages = 1

            if response.status_code != 200:
                print('error detected, retrying in 3 seconds...')
                time.sleep(3)
                errorcount += 1
                if tries < 5:
                    tries += 1
                    continue
                elif tries == 5:
                    print(f'Unable to retrieve any data for {item}. Moving on to the next...')
                    break

            data = response.json()

            if data:
                # Append the type_id to each item in the response
                for entry in data:
                    entry['type_id'] = item  # Add type_id to each record

                all_history.extend(data)
            else:
                print(f"Empty response for type_id {item}. Skipping.")

            successful_returns += 1
            max_pages = 1

        print(f'Type id {item} retrieved successfully. So far, {successful_returns} items retrieved successfully. With {errorcount} errors.')
        print('-------------------------')

        page = 1
        max_pages = 1
        print(datetime.now())

        time.sleep(.5)

    return all_history

#process the market orders

def filterorders(ids, list_orders):
    filtered_orders = list_orders[list_orders['type_id'].isin(ids)]
    return filtered_orders


def aggregate_sell_orders(orders_data):
    sell_orders = orders_data[orders_data['is_buy_order'] == False]

    grouped_df = sell_orders.groupby('type_id')['volume_remain'].sum().reset_index()
    grouped_df.columns = ['type_id', 'total_volume_remain']

    min_price_df = sell_orders.groupby('type_id')['price'].min().reset_index()
    min_price_df.columns = ['type_id', 'min_price']

    percentile_5th_df = sell_orders.groupby('type_id')['price'].quantile(0.05).reset_index()
    percentile_5th_df.columns = ['type_id', 'price_5th_percentile']

    merged_df = pd.merge(grouped_df, min_price_df, on='type_id')
    merged_df = pd.merge(merged_df, percentile_5th_df, on='type_id')

    return merged_df

def mergehistorystats(merged_orders, history_data):
    historical_df = history_data
    historical_df['date'] = pd.to_datetime(historical_df['date'])
    last_30_days_df = historical_df[historical_df['date'] >= pd.to_datetime('today') - pd.DateOffset(days=30)]
    grouped_historical_df = last_30_days_df.groupby('type_id').agg(
        avg_of_avg_price=('average', 'mean'),
        avg_daily_volume=('volume', 'mean'),
    ).reset_index()
    grouped_historical_df['avg_of_avg_price'] = grouped_historical_df['avg_of_avg_price'].round(2)
    grouped_historical_df['avg_daily_volume'] = grouped_historical_df['avg_daily_volume'].round(2)
    final_df = pd.merge(merged_orders, grouped_historical_df, on='type_id', how='left')

    return final_df



# Step 5: Main function to run the script
if __name__ == '__main__':
    # Location of data needed to process orders
    start_time = datetime.now()
    print(start_time)


    #pull market orders logging start time and checking for test mode
    print("starting data pull...market orders")

    #Configure to run in an abbreviated test mode....
    test_mode = True #uses abbreviated ESI calls for debugging

    market_orders, errorlog = fetch_market_orders(test_mode)

    # Save market orders to CSV
    print("saving market orders")
    save_to_csv(market_orders)
    save_error_log_to_csv(errorlog)

    Mkt_time_to_complete = datetime.now() - start_time
    print(f'done. Time to complete market orders: {Mkt_time_to_complete}')

    #variables for live or test mode
    testidslocation = 'data/type_ids2.csv.' #abbreviated set of ids for debugging
    liveidslocation = 'data/type_ids.csv.'

    if test_mode:
        idslocation = testidslocation
    else:
        idslocation = liveidslocation

    #code for retrieving type ids
    type_idsCSV = pd.read_csv(idslocation)
    type_ids = type_idsCSV['type_ids'].tolist()
    order_list = {}
    filtered_orders = {}

    #code for retrieving live market orders
    orders = pd.DataFrame(market_orders)

    #update history data
    print("updating history data")
    print(datetime.now())
    history_start = datetime.now()
    historical_df = pd.DataFrame(fetch_market_history(type_ids))

    history_filename = f"output/valemarkethistory_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    historical_df.to_csv(history_filename, index=False)

    hist_time_to_complete = datetime.now() - history_start
    print(f"history data saved. Time to complete: {hist_time_to_complete}")


    new_filtered_orders = filterorders(type_ids, orders)
    merged_sell_orders = aggregate_sell_orders(new_filtered_orders)
    final_data = mergehistorystats(merged_sell_orders, historical_df)

    filename = f"output/valemarketstats_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"

    output_data_location = filename
    final_data.to_csv(output_data_location, index=False)

    print(f'data processed and writing to file: {output_data_location}')

    finish_time = datetime.now()
    total_time = finish_time -start_time

    print("===================================================")
    print("ESI Request Completed Successfully.")
    print(f"Data for {len(final_data)} items retrieved.")
    print("=====================================================")
    print(f"Time to complete:\nMARKET ORDERS: {Mkt_time_to_complete}\nMARKET_HISTORY: {hist_time_to_complete}")
    print(f"TOTAL TIME TO COMPLETE: {total_time}")

    #now we optionally update the source file for our spreadsheet in the latest folder
    # and clean up other files by moving them all to the archive folder

    print("-----------cleaning up files and exiting----------------")

    src_folder = r"/output"
    latest_folder = os.path.join(src_folder, "latest")
    archive_folder = os.path.join(src_folder, "archive")
    rename_move_and_archive_csv(src_folder, latest_folder, archive_folder)

    print("market update complete")

