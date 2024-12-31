import json
import logging
import os
import time
from datetime import datetime
from typing import Any

import pandas as pd
import requests
from pandas.core.interchange.dataframe_protocol import DataFrame
from requests import ReadTimeout

import google_sheet_updater
from ESI_OAUTH_FLOW import get_token
from doctrine_monitor import read_doctrine_watchlist, get_doctrine_status_optimized
from file_cleanup import rename_move_and_archive_csv
from get_jita_prices import get_jita_prices
from logging_tool import configure_logging
from sql_handler import process_esi_market_order_optimized, read_sql_watchlist, read_history, update_stats

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

logger = configure_logging("mkt_structures", "logs/esi_mkt.log")

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
    error_count = 0
    total_pages = 0
    all_orders = []
    failed_pages = []
    failed_pages_count = 0

    logger.info("fetching orders...")  # Track status

    while page <= max_pages:
        response = requests.get(MARKET_STRUCTURE_URL + str(page), headers=headers)

        if "X-Pages" in response.headers:
            max_pages = int(response.headers["X-Pages"])
        elif response.status_code == 200:
            max_pages = 1

        page_ratio: float = page / max_pages
        page_ratio_rounded: int = round(page_ratio * 100)
        page_ratio_rounded_str: str = str(page_ratio_rounded) + "%"
        print(f"\rFetching market order pages pages {page_ratio_rounded_str}. Page: {page}", end="")

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
            logger.error(
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
                logger.error(f"Error decoding JSON response from page {page}.")
                failed_pages.append([page, "ValueError", "ValueError"])
                failed_pages_count += 1
                continue
        page += 1

        if not orders:
            break

        all_orders.extend(orders)

    if failed_pages_count > 0:
        print(f"The following pages failed: {failed_pages}")
        print(f"{failed_pages_count} pages failed.")
    else:
        print(f"All pages fetched successfully.")

    with open('output/latest/all_orders.json', 'w') as f:
        json.dumps(all_orders)

    logger.info(
        f"done. successfully retrieved {len(all_orders)}...")
    return all_orders

# update market history
def fetch_market_history(fresh_data: bool == True) -> tuple[DataFrame, list[Any] | None]:
    watchlist = read_sql_watchlist()
    type_id_list = watchlist["type_id"].unique().tolist()

    # Create a lookup dictionary for type_names only used in status update....
    type_id_to_name_map = watchlist.set_index('type_id')['type_name'].to_dict()

    if fresh_data:
        logging.info('fetching fresh data from ESI')
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

        logger.info('fetching market history for 4-HWWF')
        # Iterate over watchlist to fetch market history for 4-HWWF

        total_items = len(type_id_list)

        for type_id in range(total_items):
            while page <= max_pages:
                item = type_id_list[type_id]
                type_name = type_id_to_name_map.get(item)

                try:
                    response = requests.get(
                        market_history_url + str(item), headers=headers, timeout=timeout)

                    item_ratio: float = successful_returns / total_items
                    item_ratio_rounded: int = round(item_ratio * 100)
                    item_ratio_rounded_str: str = str(item_ratio_rounded) + "%"
                    print(f"\rFetching history {item_ratio_rounded_str} :: ({item} - {type_name})", end="")

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

        historical_df: DataFrame = pd.DataFrame(all_history)

    else:
        logger.info('retrieving cached market history data')
        historical_df = read_history(30)
        all_history = None

    logger.info(f"history data complete. {len(historical_df)} records retrieved.")
    logger.info("returning history_df")

    return historical_df, all_history
# ===============================================
# Functions: Process Market Stats
# -----------------------------------------------
def aggregate_sell_orders(market_orders_json: any) -> pd.DataFrame:
    logger.info("aggregating sell orders")
    orders = pd.DataFrame(market_orders_json)

    ids = read_sql_watchlist()
    ids = ids["type_id"].tolist()

    filtered_orders = orders[orders["type_id"].isin(ids)]
    sell_orders = filtered_orders[filtered_orders["is_buy_order"] == False]

    logger.info(f'filtered orders: {len(filtered_orders)}')
    logger.info(f'sell orders: {len(sell_orders)}')
    logger.info("aggregating orders")

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
    logger.info("successfully merged dataframes and completed aggregation")
    logger.info(f"returning merged dataframe with {len(merged_df)} rows")
    return merged_df

def merge_market_stats(merged_orders: pd.DataFrame, history_data: pd.DataFrame):
    logger.info("merging historical data")
    grouped_historical_df = history_merge(history_data)
    grouped_historical_df['type_id'] = grouped_historical_df['type_id'].astype('int64')

    merged_data = pd.merge(
        merged_orders, grouped_historical_df, on="type_id", how="left"
    )

    logger.info('filtering orders to watchlist')
    final_df = pd.merge(merged_data, watchlist, on="type_id", how="left")

    logger.info('calculating days remaining')
    final_df["days_remaining"] = final_df.apply(
        lambda row: 0 if row["avg_daily_volume"] == 0 else row["total_volume_remain"] / row["avg_daily_volume"],
        axis=1
    )

    final_df["days_remaining"] = final_df["days_remaining"].round(1)
    logger.info('merge finished. returning final_df')
    return final_df

def history_merge(history_data: pd.DataFrame) -> pd.DataFrame:
    logger.info("processing historical data")
    historical_df = history_data

    historical_df["date"] = pd.to_datetime(historical_df["date"], errors="coerce")

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

def update_doctrine_status(target: int = 20):
    target_df = get_doctrine_status_optimized(target=target)
    status = google_sheet_updater.google_sheet_updater_doctrine_items(target_df)
    logger.info(status)
    target_df.to_csv("output/latest/target_doctrines.csv", index=False)
    print("Completed doctrines check")


def process_orders(market_orders, history_data) -> tuple[DataFrame, DataFrame]:
    logger.info("aggregating sell orders")
    merged_sell_orders = aggregate_sell_orders(market_orders)

    logger.info("merging historical data")
    final_data = merge_market_stats(merged_sell_orders, history_data)

    print(type(final_data))
    logger.info("getting jita prices")
    vale_jita = get_jita_prices(final_data)
    return vale_jita, final_data

def save_data(history: DataFrame, vale_jita: DataFrame, final_data: DataFrame, fresh_data: bool = True):
    update_time = datetime.now()
    if fresh_data:
        new_columns = [
            "date",
            "type_id",
            "highest",
            "lowest",
            "average",
            "order_count",
            "volume",
        ]
        history = history[new_columns]
        history[update_time] = update_time
        history.to_csv(history_filename, index=False)

    final_data.to_csv(market_stats_filename, index=False)

    logger.info(print('saving market stats to database'))
    status = update_stats(final_data)
    logger.info(print(status))
    google_sheet_updater.google_sheet_updater()
    # save a copy of market stats to update spreadsheet consistently named
    src_folder = r"output"
    latest_folder = os.path.join(src_folder, "latest")
    archive_folder = os.path.join(src_folder, "archive")
    # cleanup files. "Full cleanup true" moves old files from output to archive.
    rename_move_and_archive_csv(src_folder, latest_folder, archive_folder, True)

    logger.info("saving vale_jita data")
    vale_jita.to_csv("output/latest/vale_jita.csv", index=False)


    print(status)


if __name__ == "__main__":
    # ===============================================
    # MAIN PROGRAM
    # -----------------------------------------------
    # Main function where everything gets executed.

    # Update full market history with fresh data?
    fresh_data_choice = False

    start_time = datetime.now()
    logger.info(f"starting program: {start_time}")

    # retrieve current watchlist from database
    logger.info(f"reading watchlist from database")
    watchlist = read_sql_watchlist()
    doctrine_watchlist = read_doctrine_watchlist('wc_fitting')
    logger.info(f"retrieved {len(watchlist)} type_ids. watchlist is:  {type(watchlist)}")

    print("MARKET ORDERS")
    logger.info("starting update...market orders")
    # =========================================
    market_orders = fetch_market_orders()
    # ==========================================

    logger.info("saving to database...market orders")
    orders_status = process_esi_market_order_optimized(market_orders, False)
    logger.info(orders_status)

    # update history data
    print("HISTORY CHECKS")
    logger.info("updating history data")
    # =============================================
    historical_df, all_history = fetch_market_history(fresh_data_choice)
    # ==============================================
    # #save to database

    if fresh_data_choice:
        logger.info(
            f"done. successfully retrieved {len(all_history)}. connecting to database...")
        history_status = process_esi_market_order_optimized(all_history, True)
        logger.info(history_status)

    #process market orders
    print('processing orders')
    logger.info("processing orders")
    vale_jita, final_data = process_orders(market_orders, historical_df)
    # check doctrine market status

    save_data(historical_df, vale_jita, final_data, fresh_data_choice)

    print("DOCTRINE CHECKS")
    logger.info('Checking doctrines')
    # =========================================
    update_doctrine_status()
    # =========================================
    # Completed stats
    finish_time = datetime.now()
    total_time = finish_time - start_time
    logger.info(f"""
    start time: {start_time}
    finish time: {finish_time}
    ---------------------------
    total time: {total_time}
    """)

    print("===================================================")
    print("ESI Request Completed Successfully.")
    print("=====================================================")

    logger.info(f"Data for {len(final_data)} items retrieved.")
    logger.info(f"Total time: {total_time}")
    logger.info("market update complete")
