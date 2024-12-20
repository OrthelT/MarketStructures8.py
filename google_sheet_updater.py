import gspread
from google.oauth2.service_account import Credentials
import pandas as pd

import db_handler

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
    # Open the Google Sheet by name
    sheet = gc.open("4H Market Status").sheet1  # Replace with your sheet name
    # Step 3: Update the sheet
    # Example: Write a Pandas DataFrame to the Google Sheet
    # data = {
    #     "Name": ["Alice", "Bob", "Charlie"],
    #     "Score": [85, 91, 78],
    # }
    # df = pd.DataFrame(data)
    # Convert DataFrame to a list of lists
    data_list = [df.columns.tolist()] + df.values.tolist()

    # Clear the existing content in the sheet (optional)
    sheet.clear()
    # Update the sheet with new data
    sheet.update(data_list)
    message = "Data updated successfully!"
    return message


if __name__ == "__main__":
    market_stats = db_handler.read_market_stats()
    cols = market_stats.columns.tolist()
    new_cols = ['type_id', 'type_name', 'group_name', 'days_remaining', 'price_5th_percentile', 'total_volume_remain',
                'min_price',
                'avg_of_avg_price', 'avg_daily_volume', 'group_id', 'category_id', 'category_name', 'timestamp']
    market_stats = market_stats[new_cols]
    google_sheet_updater(market_stats)
