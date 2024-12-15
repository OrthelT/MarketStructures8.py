import logging
import os
import time
from datetime import datetime

import pandas as pd
import requests
from requests import ReadTimeout

import db_handler as dbhandler
from backupfolder.Doctrine_check import update_doctrines
from ESI_OAUTH_FLOW import get_token
from file_cleanup import rename_move_and_archive_csv
from get_jita_prices import get_jita_prices
from logging_tool import configure_logging
from sql_handler import process_esi_market_order

# GNU General Public License
#
# ---------------------------------------------
# ESI Structure Market Tools for Eve Online
# ---------------------------------------------
# #Developed as a learning project, to access Eve's enfeebled ESI. I'm not a real programmer, ok? Don't laugh at me.
# Contact orthel_toralen on Discord with questions.

# load environment, where we store our client id and secret key.
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

# Currently set for the 4-HWWF Keepstar. You can enter another structure ID for a player-owned structure that you have access to.
structure_id = 1035466617946

# set variables for ESI requests
MARKET_STRUCTURE_URL = (
    f"https://esi.evetech.net/latest/markets/structures/{structure_id}/?page="
)
print(MARKET_STRUCTURE_URL)
SCOPE = [
    "esi-markets.structure_markets.v1"
]  # make sure you have this scope enabled in you ESI Dev Application settings.

# output locations
# You can change these file names to be more accurate when pulling data for other regions.
orders_filename = (
    f"output/4Hmarketorders_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
)
errorlog_filename = (
    f"output/Hmarketorders_errorlog_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
)
history_filename = (
    f"output/valemarkethistory_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
)
market_stats_filename = (
    f"output/valemarketstats_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
)
merged_sell_filename = (
    f"output/valemergedsell_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
)
master_history_filename = "data/masterhistory/valemarkethistory_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"

configure_logging("mkt_structures", "logs/esi_mkt.log")


# ===============================================
# Functions: Fetch Market Structure Orders
# -----------------------------------------------
def fetch_market_orders():
    # initiates the oath2 flow
    token = get_token(SCOPE)
    print("ESI Scope Authorized. Requesting data.")
    print("-----------------------------------------")

    headers = {
        "Authorization": f'Bearer {token["access_token"]}',
        "Content-Type": "application/json",
    }

    page = 1
    max_pages = 1
    tries = 0
    total_tries = 0
    error_count = 0
    total_pages = 0
    all_orders = []
    failed_pages = []
    failed_pages_count = 0

    logging.info("fetching orders...")  # Track status

    while page <= max_pages:
        print(f"\rFetching page {page}...", end="")
        response = requests.get(MARKET_STRUCTURE_URL + str(page), headers=headers)

        if "X-Pages" in response.headers:
            max_pages = int(response.headers["X-Pages"])
        elif response.status_code == 200:
            max_pages = 1

        # make sure we don't hit the error limit and get our IP banned
        errorsleft = int(response.headers.get("X-ESI-Error-Limit-Remain", 0))
        errorreset = int(response.headers.get("X-ESI-Error-Limit-Reset", 0))
        if errorsleft == 0:
            break
        elif errorsleft < 10:
            print(
                f"WARNING: Errors remaining: {errorsleft}. Error limit reset: {errorreset} seconds."
            )

        # some error handling to gently keep prodding the ESI until we gat all the data
        if response.status_code != 200:
            error_code = response.status_code
            error_details = response.json()
            error = error_details["error"]
            logging.error(
                f"Error fetching data from page {page}. status code: {error_code}"
            )
            error_count += 1

            if tries < 5:
                tries += 1
                time.sleep(3)
                continue
            else:
                print(f"Reached the 5th try and giving up on page {page}.")
                failed_pages.append([page, error_code, error])
                failed_pages_count += 1
                page += 1
                tries = 0
                continue
        else:
            tries = 0
            total_pages += 1
            try:
                orders = response.json()
            except ValueError:
                logging.error(f"Error decoding JSON response from page {page}.")
                failed_pages.append([page, "ValueError", "ValueError"])
                failed_pages_count += 1
                continue
        page += 1

        if not orders:
            break

        all_orders.extend(orders)

    print("-----------------------------------------------")
    print("Market Orders Complete")
    print("-----------------------------------------------")
    print("SUMMARY:")
    print(f"Fetched pages: {total_pages}")
    print(f"Total orders: {len(all_orders)}")
    print(f"Errors: {error_count}")
    if failed_pages_count > 0:
        print(f"The following pages failed: {failed_pages}")
        print(f"{failed_pages_count} pages failed.")
    else:
        print(f"All pages fetched successfully.")
    print("Returning all orders....")
    print("-----------------------------------------------")
    print("================================================")
    return all_orders


# update market history
def fetch_market_history(type_id_list: list[int]) -> list[dict[str, int | str | float]]:
    timeout = 10
    market_history_url = "https://esi.evetech.net/latest/markets/10000003/history/?datasource=tranquility&type_id="

    headers = {
        "Content-Type": "application/json",
    }
    all_history = []
    page = 1
    max_pages = 1
    errorcount = 0
    tries = 0
    successful_returns = 0

    print("----------------------------")
    print("Updating market history...")
    print("============================")
    # Iterate over type_ids to fetch market history for 4-HWWF
    for type_id in range(len(type_id_list)):
        while page <= max_pages:
            item = type_id_list[type_id]
            print(
                f"\ritems retrieved: {successful_returns}/{len(type_id_list)}", end=""
            )

            try:
                response = requests.get(
                    market_history_url + str(item), headers=headers, timeout=timeout
                )
                page += 1

                if "X-Pages" in response.headers:
                    max_pages = int(response.headers["X-Pages"])
                else:
                    max_pages = 1

                if response.status_code != 200:
                    logging.error("error detected, retrying in 3 seconds...")
                    time.sleep(3)
                    errorcount += 1
                    if tries < 5:
                        tries += 1
                        continue
                    elif tries == 5:
                        print(
                            f"Unable to retrieve any data for {item}. Moving on to the next..."
                        )
                        page = 1
                        break

                data = response.json()

                if data:
                    # Append the type_id to each item in the response
                    for entry in data:
                        entry["type_id"] = item  # Add type_id to each record
                    all_history.extend(data)
                else:
                    logging.info(f"Empty response for type_id {item}. Skipping.")

                successful_returns += 1
                tries = 0
                max_pages = 1

            except ReadTimeout:
                logging.info(
                    f"Request timed out for page {page}, item {item}. Retrying..."
                )
                if tries < 5:
                    tries += 1
                    time.sleep(3)  # Wait before retrying
                    continue

        page = 1
        max_pages = 1

    logging.info(
        f"HISTORY COMPLETE: Items found: {successful_returns} Errors:{errorcount}"
    )
    print("==========HISTORY COMPLETE==============")
    logging.info("returning all_history")
    return all_history


# ===============================================
# Functions: Process Market Stats
# -----------------------------------------------
def aggregate_sell_orders(market_orders_json: any, ids: list[int]) -> pd.DataFrame:
    orders = pd.DataFrame(market_orders_json)
    logging.info("filtering orders")
    filtered_orders = orders[orders["type_id"].isin(ids)]
    sell_orders = filtered_orders[filtered_orders["is_buy_order"] == False]

    logging.info("aggregating orders")
    grouped_df = sell_orders.groupby("type_id")["volume_remain"].sum().reset_index()
    grouped_df.columns = ["type_id", "total_volume_remain"]
    min_price_df = sell_orders.groupby("type_id")["price"].min().reset_index()
    min_price_df.columns = ["type_id", "min_price"]
    percentile_5th_df = (
        sell_orders.groupby("type_id")["price"].quantile(0.05).reset_index()
    )
    percentile_5th_df.columns = ["type_id", "price_5th_percentile"]
    merged_df = pd.merge(grouped_df, min_price_df, on="type_id")
    merged_df = pd.merge(merged_df, percentile_5th_df, on="type_id")
    logging.info("successfully merged dataframes and completed aggregation")
    logging.info("returning merged dataframe")
    return merged_df


def doctrine_stock(target_stock: int, market_stats: pd.DataFrame) -> pd.DataFrame:
    pass


def merge_market_stats(merged_orders, history_data):
    grouped_historical_df = history_merge(history_data)
    merged_data = pd.merge(
        merged_orders, grouped_historical_df, on="type_id", how="left"
    )
    watchlist_names = watchlist.copy()
    final_df = pd.merge(merged_data, watchlist_names, on="type_id", how="left")
    return final_df


def history_merge(history_data):
    logging.info("processing historical data")
    historical_df = history_data
    historical_df["date"] = pd.to_datetime(historical_df["date"])
    last_30_days_df = historical_df[
        historical_df["date"] >= pd.to_datetime("today") - pd.DateOffset(days=30)
        ]
    grouped_historical_df = (
        last_30_days_df.groupby("type_id")
        .agg(
            avg_of_avg_price=("average", "mean"),
            avg_daily_volume=("volume", "mean"),
        )
        .reset_index()
    )
    grouped_historical_df["avg_of_avg_price"] = grouped_historical_df[
        "avg_of_avg_price"
    ].round(2)
    grouped_historical_df["avg_daily_volume"] = grouped_historical_df[
        "avg_daily_volume"
    ].round(2)
    logging.info("history data processed. returning grouped historical data")
    return grouped_historical_df


if __name__ == "__main__":
    # ===============================================
    # MAIN PROGRAM
    # -----------------------------------------------
    # Main function where everything gets executed.
    start_time = datetime.now()
    logging.info(f"starting program: {start_time}")
    # retrieve current type_ids from database
    logging.info("reading watchlist from database")
    watchlist = dbhandler.read_watchlist()
    type_ids = watchlist.typeID.tolist()
    logging.info(f"retrieved {len(type_ids)} type_ids")
    print("------------------")
    print("MARKET ORDERS")
    print("-----------------")
    # pull market orders
    logging.info("starting update...market orders")
    market_orders = fetch_market_orders()

    logging.info(
        f"done. successfully retrieved {len(market_orders)}. connecting to database..."
    )

    # save to database
    logging.info("saving to database...market orders")
    df_status = process_esi_market_order(market_orders, False)
    logging.info(df_status)

    print("------------------")
    print("MARKET HISTORY")
    print("------------------")

    # update history data
    logging.info("updating history data")
    raw_history = fetch_market_history(type_ids)
    print("connecting to database and updating data")

    # #save to database
    logging.info("saving history data to database")
    df_status = process_esi_market_order(raw_history, True)
    logging.info(df_status)
    logging.info("updating doctrine tracking")
    doct_df = update_doctrines()
    print(doct_df.head())

    historical_df = pd.DataFrame(raw_history)
    logging.info(f"history data complete. {len(historical_df)} records retrieved.")

    print("------------------")
    print("PROCESSING ORDERS")
    print("-----------------")
    # processing data
    watchlist.rename(columns={"typeID": "type_id"}, inplace=True)

    logging.info("aggregating sell orders")
    merged_sell_orders = aggregate_sell_orders(market_orders, type_ids)
    logging.info("merging historical data")
    final_data = merge_market_stats(merged_sell_orders, historical_df)
    logging.info("getting jita prices")
    vale_jita = get_jita_prices(final_data)

    # save files
    logging.info("-----------saving files and exiting----------------")
    # reorder history columns
    new_columns = [
        "date",
        "type_id",
        "highest",
        "lowest",
        "average",
        "order_count",
        "volume",
    ]
    historical_df = historical_df[new_columns]
    historical_df.to_csv(history_filename, index=False)
    final_data.to_csv(market_stats_filename, index=False)

    # save a copy of market stats to update spreadsheet consistently named
    src_folder = r"output"
    latest_folder = os.path.join(src_folder, "latest")
    archive_folder = os.path.join(src_folder, "archive")
    # cleanup files. "Full cleanup true" moves old files from output to archive.
    rename_move_and_archive_csv(src_folder, latest_folder, archive_folder, True)

    logging.info("saving vale_jita data")
    vale_jita.to_csv("output/latest/vale_jita.csv", index=False)
    # Completed stats
    finish_time = datetime.now()
    total_time = finish_time - start_time

    print("===================================================")
    print("ESI Request Completed Successfully.")
    logging.info(f"Data for {len(final_data)} items retrieved.")
    logging.info(f"Total time: {total_time}")
    print("=====================================================")
    logging.info("market update complete")
