import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive", ]

SERVICE_ACCOUNT_FILE = "replace_with_your_file.json"  # Path to JSON file with your google service account credentials

def get_credentials(SCOPES: list):
    # Step 1: Set up credentials and authenticate
    credentials = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    # Step 2: Connect to Google Sheets
    gc = gspread.authorize(credentials)
    return gc

def google_mkt_sheet_updater(df) -> str:
    gc = get_credentials(SCOPES)
    # Clean the DataFrame to ensure JSON compliance
    df = df.infer_objects()
    df2 = fill_na(df) #this function just handles any null values

    # Convert DataFrame to a list of lists (Google Sheets format)
    data_list = [df2.columns.tolist()] + df2.astype(str).values.tolist()

    # Open the Google Sheet Workbook by name by default it will select the first sheet
    wb = gc.open("Your_Market_Sheet_Name") #Modify these variables to match your Sheets workbook
    sheet = wb.worksheet("Your_Sheet_Name")

    try:
        # Clear the existing content in the sheet
        sheet.clear()
        # Update the sheet with new data, starting at cell A1
        result = sheet.update(
            values=data_list, range_name='A1')
        message = "Market Stats data updated successfully!"

    except Exception as e:
        message = f"An error occurred while updating MarketStats: {str(e)}"
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

if __name__ == "__main__":
    pass