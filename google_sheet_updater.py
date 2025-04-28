
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

from sql_handler import read_sql_market_stats, read_sql_watchlist
import logging_tool

logger = logging_tool.configure_logging(__name__)

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


def google_mkt_sheet_updater() -> str:
    gc = get_credentials(SCOPES)

    df = read_sql_market_stats()

    new_cols = ['type_id', 'type_name', 'days_remaining', 'total_volume_remain', 'price_5th_percentile',
                'avg_daily_volume', 'avg_of_avg_price', 'min_price', 'group_name', 'category_name',
                'group_id', 'category_id', 'timestamp']
    df = df[new_cols]
    renamed_cols = ['type_id', 'name', 'days', 'Qty on Mkt', '4H Sell', 'avg volume', 'avg price', 'min price',
                    'group', 'category', 'group_id', 'category_id', 'updated_at']
    col_map = dict(zip(new_cols, renamed_cols))
    df = df.rename(columns=col_map)

    # Clean the DataFrame to ensure JSON compliance
    df = df.infer_objects()
    df2 = fill_na(df)
    # Convert DataFrame to a list of lists (Google Sheets format)
    data_list = [df2.columns.tolist()] + df2.astype(str).values.tolist()

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
        logger.warning(message)
        raise
    return message


def google_sheet_updater_doctrine_items(df: pd.DataFrame) -> str:

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
        logger.info(result)
        message = "Doctrine items data updated successfully!"
        logger.info(print(message, result))
    except Exception as e:
        message = f"An error occurred while updating doctrine items: {str(e)}"
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
    # utility function to post a table of URLs to use for icons in dashboard views
    gc = get_credentials(SCOPES)

    watchlist = read_sql_watchlist()
    type_ids = watchlist['type_id'].tolist()
    df = pd.DataFrame()

    for type_id in type_ids:
        url = f"https://images.evetech.net/types/{type_id}/render?size=64"
        df = pd.concat([df, pd.DataFrame({"type_id": [type_id], "URLs": [url]})], ignore_index=True)
        # Convert DataFrame to a list of lists (Google Sheets format)
    data_list = [df.columns.tolist()] + df.astype(str).values.tolist()
    wb = gc.open("4H Market Status")
    sheet = wb.worksheet("URLs")
    try:
        # Clear the existing content in the sheet
        sheet.clear()
        # Update the sheet with new data, starting at cell A1
        result = sheet.update('A1', data_list)
        logger.info(result)

    except Exception as e:
        # Handle errors gracefully
        result = f"An error occurred while updating short items: {str(e)}"
        logger.error(result)
        raise

    return result


if __name__ == "__main__":
    df = google_mkt_sheet_updater()
    print(df['type_id'].unique())
