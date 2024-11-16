import os

import requests
import webbrowser
import time
import csv
import pandas as pd
import json
from requests import ReadTimeout
from datetime import datetime
from requests_oauthlib import OAuth2Session
from dotenv import load_dotenv

from file_cleanup import rename_move_and_archive_csv

# LICENSE
# This program is free software: you can redistribute it and/or modify it under the terms of the
# GNU General Public License as published by the Free Software Foundation, either version 3 of the License,
# or (at your option) any later version. This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License for more details. <https://www.gnu.org/licenses/>.
#
# ---------------------------------------------
# ESI Structure Market Tools for Eve Online
# ---------------------------------------------
# #Developed as a learning project, to access Eve's enfeebled ESI. I'm not a real programmer, ok? Don't laugh at me.
# Contact orthel_toralen on Discord with questions.

# load environment, where we store our client id and secret key.
load_dotenv()

# This flags Eve SSO to let us use a standard HTTP:// connection to do our Eve login.
# Eve's SSO wants a https:// connection. But, who has time for setting that up. We're just going to use localhost:8000
# Probably not ideal to transmit credentials across plain HTTP, so use your own judgement.
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

# Currently set for the 4-HWWF Keepstar. You can enter another structure ID for a player-owned structure that you have access to.
structure_id = 1035466617946

# set variables for ESI requests
CLIENT_ID = os.getenv('CLIENT_ID')  # stored in you .env file
SECRET_KEY = os.getenv('SECRET_KEY')  # stored in you .env file
REDIRECT_URI = 'http://localhost:8000/callback'  #workaround so we don't have to set up a real server
AUTHORIZATION_URL = 'https://login.eveonline.com/v2/oauth/authorize'
TOKEN_URL = 'https://login.eveonline.com/v2/oauth/token'
MARKET_STRUCTURE_URL = f'https://esi.evetech.net/latest/markets/structures/{structure_id}/?page='
SCOPE = [
    'esi-markets.structure_markets.v1']  #make sure you have this scope enabled in you ESI Dev Application settings.
token_file = 'token.json'

# output locations
# You can change these file names to be more accurate when pulling data for other regions.
orders_filename = f"output/4Hmarketorders_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
errorlog_filename = f"output/Hmarketorders_errorlog_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
history_filename = f"output/valemarkethistory_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
market_stats_filename = f"output/valemarketstats_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
merged_sell_filename = f"output/valemergedsell_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"


def debug_mode():
    test_choice = input("run in testing mode? This will use abbreviated ESI calls for quick debugging (y/n):")
    if test_choice == 'y':
        test_mode = True  # uses abbreviated ESI calls for debugging
        csv_save_mode = input("save output to CSV? (y/n):")
        if csv_save_mode == 'y':
            csv_save_mode = True
        else:
            csv_save_mode = False
    else:
        test_mode = False
        csv_save_mode = True

    return test_mode, csv_save_mode
#===============================================
# Functions: Oauth2 Flow
#-----------------------------------------------

# noinspection PyTypeChecker
def save_token(token):
    # Save the OAuth token including refresh token to a file.
    with open(token_file, 'w') as f:
        json.dump(token, f)
        # note some IDEs will flag this as an error.
        # This is because jason.dump expects a str, but got a TextIO instead.
        # TextIO does support string writing, so this is not actually an issue.


def load_token():
    # Load the OAuth token from a file, if it exists.
    if os.path.exists(token_file):
        with open(token_file, 'r') as f:
            return json.load(f)
    return None


def get_oauth_session(token=None):
    # Get an OAuth session, refreshing the token if necessary.
    extra = {'client_id': CLIENT_ID, 'client_secret': SECRET_KEY}
    if token:
        return OAuth2Session(CLIENT_ID, token=token, auto_refresh_url=TOKEN_URL, auto_refresh_kwargs=extra,
                             token_updater=save_token)
    else:
        return OAuth2Session(CLIENT_ID, redirect_uri=REDIRECT_URI, scope=SCOPE)


# Redirect user to the EVE Online login page to get the authorization code.
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

#===============================================
# Functions: Fetch Market Structure Orders
#-----------------------------------------------
def fetch_market_orders(test_mode):

    #initiates the oath2 flow
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

        #The test mode booleon sets a limited number of pages for debugging.
        if test_mode:
            max_pages = 3

        #make sure we don't hit the error limit and get our IP banned
        errorsleft = int(response.headers.get('X-ESI-Error-Limit-Remain', 0))
        errorreset = int(response.headers.get('X-ESI-Error-Limit-Reset', 0))

        if errorsleft == 0:
            break
        elif errorsleft < 10:
            print(f'WARNING: Errors remaining: {errorsleft}. Error limit reset: {errorreset} seconds.')

        #some error handling to gently keep prodding the ESI until we gat all the data
        if response.status_code != 200:
            error_code = response.status_code
            print(
                f"Error fetching data from page {page}. status code: {error_code}. tries: {tries} Retrying in 3 seconds...")
            print(os.strerror(error_code))

            error_count += 1

            #error logging
            if error_code not in errorlog:
                errorlog[error_code] = []  # Initialize the list if it doesn't exist
            errorlog[error_code].append(page)

            if tries < 5:
                tries += 1
                time.sleep(.5)
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
        time.sleep(.5)

    print(f"Retrieval complete. Fetched {total_pages}. Total orders: {len(all_orders)}")
    print(f"Received {error_count} errors. After {total_tries} total tries.")
    print("Returning all orders....")

    return all_orders, errorlog


# Save the CSV files
# noinspection PyTypeChecker
def save_to_csv(orders, filename):
    fields = ['type_id', 'order_id', 'price', 'volume_remain', 'volume_total', 'is_buy_order', 'issued', 'range']

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
    # note some IDEs will flag the variable 'file' as an error.
    # This is because DictWriter expects a str, but got a TextIO instead.
    # TextIO does support string writing, so this is not actually an issue.


def save_error_log_to_csv(errorlog, filename=None):

    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)

        # Write the header
        writer.writerow(['Error Code', 'Pages'])

        # Write each error code and the pages it occurred on
        for error_code, pages in errorlog.items():
            pages_str = ', '.join(map(str, pages))  # Convert list of pages to a comma-separated string
            writer.writerow([error_code, pages_str])
    print(f"Error log saved to {filename}")


# Get the type ids to use for market history
def get_type_ids(datafile):
    watchlist = pd.read_csv(datafile,
                            usecols=[0],
                            names=['type_id'], skiprows=1
                            )
    type_ids = watchlist['type_id'].tolist()
    return type_ids

# update market history
def fetch_market_history(type_id_list: list[int]) -> list[dict[str, int | str | float]]:
    timeout = 10
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

    # Iterate over type_ids to fetch market history for 4-HWWF
    for type_id in range(len(type_id_list)):
        while page <= max_pages:
            item = type_id_list[type_id]

            try:
                response = requests.get(market_history_url + str(item), headers=headers, timeout=timeout)
                print(f"Fetching type_id: {item}, status code: {response.status_code}")  # Add this line to track status
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

            except ReadTimeout:
                print(f"Request timed out for page {page}, item {item}. Retrying...")
                if tries < 5:
                    tries += 1
                    time.sleep(3)  # Wait before retrying
                    continue

            print(
                f'Type id {item} retrieved successfully. So far, {successful_returns} items retrieved successfully. With {errorcount} errors.')
            print('-------------------------')

        page = 1
        max_pages = 1

    return all_history


# ===============================================
# Functions: Process Market Stats
#-----------------------------------------------
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
    grouped_historical_df = history_merge(history_data)
    final_df = pd.merge(merged_orders, grouped_historical_df, on='type_id', how='left')
    return final_df, grouped_historical_df

def history_merge(history_data):
    historical_df = history_data
    historical_df['date'] = pd.to_datetime(historical_df['date'])
    last_30_days_df = historical_df[historical_df['date'] >= pd.to_datetime('today') - pd.DateOffset(days=30)]
    grouped_historical_df = last_30_days_df.groupby('type_id').agg(
        avg_of_avg_price=('average', 'mean'),
        avg_daily_volume=('volume', 'mean'),
    ).reset_index()
    grouped_historical_df['avg_of_avg_price'] = grouped_historical_df['avg_of_avg_price'].round(2)
    grouped_historical_df['avg_daily_volume'] = grouped_historical_df['avg_daily_volume'].round(2)

    return grouped_historical_df


def missing_history(grouped_history_data, merged_data):
    missing_history_data = []
    for index, row in grouped_history_data.iterrows():
        if row['type_id'] not in merged_data['type_id'].tolist():
            missing_history_data.append(row)
    missing_history_df = pd.DataFrame(missing_history_data)

    # merge missing history data
    final_merged_data = pd.concat([merged_data, missing_history_df])

    # fix na and number formats
    final_merged_data.fillna(0, inplace=True)
    final_merged_data['type_id'] = final_merged_data['type_id'].astype(int)

    return final_merged_data


# ===============================================
# MAIN PROGRAM
# -----------------------------------------------
# <ain function where everything gets executed.

if __name__ == '__main__':

    # hit the stopwatch to see how long it takes
    start_time = datetime.now()
    print(start_time)

    # pull market orders logging start time and checking for test mode
    print("starting data pull...market orders")

    # Configure to run in an abbreviated test mode....
    test_mode, csv_save_mode = debug_mode()

    # code for retrieving live market orders
    market_orders, errorlog = fetch_market_orders(test_mode)
    Mkt_time_to_complete = datetime.now() - start_time
    Avg_market_response_time = (Mkt_time_to_complete.microseconds / len(market_orders)) / 1000
    print(
        f'done. Time to complete market orders: {Mkt_time_to_complete}, avg market response time: {Avg_market_response_time}ms')

    # variables for live or test mode
    testidslocation = 'data/type_ids2.csv.'  # abbreviated set of ids for debugging
    liveidslocation = 'data/type_ids.csv.'

    if test_mode:
        idslocation = testidslocation
    else:
        idslocation = liveidslocation

    # code for retrieving type ids
    type_idsCSV = pd.read_csv(idslocation)
    type_ids = type_idsCSV['type_ids'].tolist()


    # update history data
    print("updating history data")
    history_start = datetime.now()
    historical_df = pd.DataFrame(fetch_market_history(type_ids))
    hist_time_to_complete = datetime.now() - history_start
    print(f"history data complete: {hist_time_to_complete}")

    # process data
    orders = pd.DataFrame(market_orders)
    new_filtered_orders = filterorders(type_ids, orders)
    merged_sell_orders = aggregate_sell_orders(new_filtered_orders)

    # merged_history = history_merge(historical_df)
    merged_data, grouped_history_data = mergehistorystats(merged_sell_orders, historical_df)

    # now we add back in history items that we don't have current sell orders for
    final_data = missing_history(grouped_history_data, merged_data)



    # save files
    if csv_save_mode:
        print("-----------saving files and exiting----------------")

        save_to_csv(market_orders, orders_filename)
        save_error_log_to_csv(errorlog, errorlog_filename)
        historical_df.to_csv(history_filename, index=False)
        final_data.to_csv(market_stats_filename, index=False)

        #save a copy of market stats to update spreadsheet consistently named
        src_folder = r"output"
        latest_folder = os.path.join(src_folder, "latest")
        archive_folder = os.path.join(src_folder, "archive")
        #cleanup files. "Full cleanup true" moves old files from output to archive.
        rename_move_and_archive_csv(src_folder, latest_folder, archive_folder, True)

    #Completed stats
    finish_time = datetime.now()
    total_time = finish_time - start_time

    print("===================================================")
    print("ESI Request Completed Successfully.")
    print(f"Data for {len(final_data)} items retrieved.")
    print("=====================================================")
    print(
        f"Time to complete:\nMARKET ORDERS: {Mkt_time_to_complete}, avg: {Avg_market_response_time}\nMARKET_HISTORY: {hist_time_to_complete}")
    print(f"TOTAL TIME TO COMPLETE: {total_time}")
    print("market update complete")
