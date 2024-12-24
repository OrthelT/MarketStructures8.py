from logging import FileHandler

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import json
import db_handler
import doctrine_monitor
import sql_handler
import logging
import datetime
from logging_tool import configure_logging

logger = configure_logging(
    "google_sheet_updater",
    "logs/google_sheet_updater.log")
logger.addHandler(logging.StreamHandler())

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive", ]

def get_credentials(SCOPES: list):
    # Step 1: Set up credentials and authenticate
    SERVICE_ACCOUNT_FILE = "data/wcdoctrines-3f38cc49f0a8.json"  # Path to JSON file
    credentials = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    # Step 2: Connect to Google Sheets
    gc = gspread.authorize(credentials)
    return gc

def google_sheet_updater() -> str:
    gc = get_credentials(SCOPES)

    df = db_handler.read_market_stats()
    new_cols = ['type_id', 'type_name', 'days_remaining', 'total_volume_remain', 'price_5th_percentile',
                'avg_daily_volume', 'avg_of_avg_price', 'min_price', 'group_name', 'category_name',
                'group_id', 'category_id', 'timestamp']
    df = df[new_cols]
    renamed_cols = ['type_id', 'name', 'days', 'Qty on Mkt', '4H Sell', 'avg volume', 'avg price', 'min price',
                    'group', 'category', 'group_id', 'category_id', 'updated_at']
    col_map = dict(zip(new_cols, renamed_cols))
    df = df.rename(columns=col_map)

    # Clean the DataFrame to ensure JSON compliance
    df = fill_na(df)
    print(df.head())
    # access credentials to update Google sheets
    # Convert DataFrame to a list of lists (Google Sheets format)
    data_list = [df.columns.tolist()] + df.astype(str).values.tolist()
    # Open the Google Sheet Workbook by name
    wb = gc.open("4H Market Status")
    sheet = wb.worksheet("MarketStats")

    try:
        # Clear the existing content in the sheet
        sheet.clear()
        # Update the sheet with new data, starting at cell A1
        result = sheet.update(
            values=data_list, range_name='A1')
        message = "Market Stats data updated successfully!"
        logger.info(message)
        logger.info(result)

    except Exception as e:
        message = f"An error occurred while updating MarketStats: {str(e)}"
        print(message)
        raise
    return message

def google_sheet_updater_short() -> str:
    # This function grabs items we have identified as being in short supply and posts to Google sheets.
    # Read data from the SQL handler
    df = sql_handler.read_short_items()
    new_cols = [
        'type_id', 'type_name', 'fits_on_market', 'quantity',
        'volume_remain', 'price',
        'delta', 'id', 'fit_id', 'doctrine_name', 'timestamp']
    # Ensure DataFrame columns are in the expected order
    df = df[new_cols]
    # Clean the DataFrame to ensure JSON compliance
    df = fill_na(df)
    # Convert DataFrame to a list of lists (Google Sheets format)
    data_list = [df.columns.tolist()] + df.astype(str).values.tolist()

    # access credentials to update Google sheets
    gc = get_credentials(SCOPES)
    # Open the Google Sheet by name
    wb = gc.open("4H Market Status")
    # Use the worksheet name to select the correct sheet
    sheet = wb.worksheet("ShortItems")
    try:
        # Clear the existing content in the sheet
        clear = sheet.clear()
        logger.info(clear)
        # Update the sheet with new data, starting at cell A1
        result = sheet.update(
            values=data_list, range_name='A1')
        print(result)
        message = "Short items data updated successfully!"
    except Exception as e:
        message = f"An error occurred while updating short items: {str(e)}"
        print(message)
        raise
    return message


def google_sheet_updater_doctrine_items() -> str:
    # This function grabs items we have identified as being in short supply and posts to Google sheets.
    # Read data from the SQL handler
    df = sql_handler.read_doctrine_items()

    new_cols = [
        'type_id', 'type_name', 'fits_on_market', 'quantity',
        'volume_remain', 'price',
        'delta', 'id', 'fit_id', 'doctrine_name', 'timestamp']
    # Ensure DataFrame columns are in the expected order
    df = df[new_cols]
    # Clean the DataFrame to ensure JSON compliance
    df = fill_na(df)
    # Convert DataFrame to a list of lists (Google Sheets format)
    data_list = [df.columns.tolist()] + df.astype(str).values.tolist()
    # access credentials to update Google sheets
    gc = get_credentials(SCOPES)
    # Open the Google Sheet by name
    wb = gc.open("4H Market Status")
    # Use the worksheet name to select the correct sheet
    sheet = wb.worksheet("DoctrineItems")
    try:
        # Clear the existing content in the sheet
        clear = sheet.clear()
        logger.info(clear)
        # Update the sheet with new data, starting at cell A1
        result = sheet.update(values=data_list, range_name='A1')
        print(result)
        message = "Doctrine items data updated successfully!"
        logger.info(print(message, result))
    except Exception as e:
        message = f"An error occurred while updating doctrine items: {str(e)}"
        print(message)
        logger.error(message)
        raise
    return message

def fill_na(df: pd.DataFrame) -> pd.DataFrame:
    # cleaning routine to keep Google Sheet API for getting grumpy about null values
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            # Replace NaN and infinite values with 0 for numeric columns
            df[col] = df[col].fillna(0).replace([float('inf'), float('-inf')], 0)
        else:
            # Replace NaN with empty string for non-numeric columns
            df[col] = df[col].fillna("").replace([float('inf'), float('-inf')], "")
    return df

def gsheet_image_updater(df: pd.DataFrame):
    #utility function to post a table of URLs to use in dashboard views
    gc = get_credentials(SCOPES)
    df = df.copy()  # Work on a copy to avoid modifying the original DataFrame
    # Convert DataFrame to a list of lists (Google Sheets format)
    data_list = [df.columns.tolist()] + df.astype(str).values.tolist()
    wb = gc.open("4H Market Status")
    sheet = wb.worksheet("URLs")
    try:
        # Clear the existing content in the sheet
        sheet.clear()

        # Update the sheet with new data, starting at cell A1
        result = sheet.update('A1', data_list)
        print(result)
        message = "image data updated successfully!"
    except Exception as e:
        # Handle errors gracefully
        message = f"An error occurred while updating short items: {str(e)}"
        print(message)
        raise

    return message


if __name__ == "__main__":
    pass