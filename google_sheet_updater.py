import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import json
import db_handler
import sql_handler
import logging
import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())
logger.addHandler(logging.FileHandler("logs/gsheets_report.log"))

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def get_credentials(SCOPES: list):
    # Step 1: Set up credentials and authenticate
    SERVICE_ACCOUNT_FILE = "data/wcdoctrines-3f38cc49f0a8.json"  # Path to JSON file
    credentials = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    # Step 2: Connect to Google Sheets
    gc = gspread.authorize(credentials)
    return gc


def google_sheet_updater(df: pd.DataFrame) -> str:
    gc = get_credentials(SCOPES)

    # Clean the DataFrame to ensure JSON compliance
    df = df.copy()  # Work on a copy to avoid modifying the original DataFrame
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            # Replace NaN and infinite values with 0 for numeric columns
            df[col] = df[col].fillna(0).replace([float('inf'), float('-inf')], 0)
        else:
            # Replace NaN with empty string for non-numeric columns
            df[col] = df[col].fillna("").replace([float('inf'), float('-inf')], "")

    # Convert DataFrame to a list of lists (Google Sheets format)
    data_list = [df.columns.tolist()] + df.astype(str).values.tolist()

    # Open the Google Sheet by name
    sheet = gc.open("4H Market Status").sheet1

    try:
        # Clear the existing content in the sheet
        sheet.clear()

        # Update the sheet with new data, starting at cell A1
        sheet.update('A1', data_list)
        print("Sheet updated successfully.")

        # Get additional information from the short updater
        short_message = google_sheet_updater_short()
        message = "Data updated successfully!\n" + short_message
    except Exception as e:
        # Handle errors gracefully and log them
        message = f"An error occurred while updating the sheet: {str(e)}"
        print(message)
        raise

    print(message)
    return message


def google_sheet_updater_short() -> str:
    gc = get_credentials(SCOPES)

    # Open the Google Sheet by name
    wb = gc.open("4H Market Status")

    # Read data from the SQL handler
    df = sql_handler.read_short_items()
    df['timestamp'] = str(datetime.datetime.now())
    new_cols = [
        'type_id', 'type_name', 'quantity',
        'volume_remain', 'price', 'fits_on_market',
        'delta', 'id', 'fit_id', 'doctrine_name', 'timestamp'
    ]

    # Ensure DataFrame columns are in the expected order
    df = df[new_cols]

    # Clean the DataFrame to ensure JSON compliance
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            # Replace NaN and infinite values with 0 for numeric columns
            df[col] = df[col].fillna(0).replace([float('inf'), float('-inf')], 0)
        else:
            # Replace NaN with empty string for non-numeric columns
            df[col] = df[col].fillna("").replace([float('inf'), float('-inf')], "")

    # Convert DataFrame to a list of lists (Google Sheets format)
    data_list = [df.columns.tolist()] + df.astype(str).values.tolist()

    # Use the worksheet name to select the correct sheet
    sheet = wb.worksheet("ShortItems")  # Replace with your worksheet name
    try:
        # Clear the existing content in the sheet
        sheet.clear()

        # Update the sheet with new data, starting at cell A1
        result = sheet.update('A1', data_list)
        print(result)
        message = "Short items data updated successfully!"
    except Exception as e:
        # Handle errors gracefully
        message = f"An error occurred while updating short items: {str(e)}"
        print(message)
        raise

    return message


if __name__ == "__main__":
    pass
