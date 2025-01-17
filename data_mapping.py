import json
import logging

import pandas as pd

from MarketStructures8 import aggregate_sell_orders
from models import DataMaps
from sql_handler import insert_pd_type_names

# Example mapping
column_mapping = {
    "type_id": ["type_id", "typeID", "type id"],
    "type_name": ["type_name", "typeName", "item"],
    "stock": ["stock", "volume_remain", "total_volume_remain", "qty on mkt"],
    "min_price": ["min_price", "min price"],
    "price": ["price", "4H price", "price_5th_percentile"],
    "avg_price": ["avg_price", "avg_of_avg_price"],
    "avg_volume": ["avg_volume", "avg daily volume", "avg_daily_volume"],
    "group": ["group", "group_name"],
    "group_id": ["group_id", "grp id"],
    "category": ["category", "category_name"],
    "category_id": ["category_id", "cat id"],
    "days": ["days", "days_remaining"],
    "timestamp": ["timestamp", "issued"],
    "fits": ["fits", "qty_on_market"],
    "fit": ["fit", "fit_name", "fit name"],
    "fit_id": ["fit_id", "fit id"],
    "doctrine": ["doctrine", "doctrine_name"],
    "doctrine_id": ["doctrine_id", "doc id"],
    "ship": ["ship", "ship_name"],
    "ship_id": ["ship_id", "hull_id"],
    "qty": ["qty", "quantity", "qty_required"]

}

# Target schema (all columns needed in DataMaps)
data_maps_schema = [
    "type_id", "type_name", "group_id", "category_id",
    "price", "avg_price", "avg_volume", "stock", "days",
    "fits", "fit", "fit_id", "doctrine", "doctrine_id",
    "ship", "ship_id", "qty", "timestamp"
]

# Configure logging
logging.basicConfig(
    filename="logs/unmapped_columns.log",
    level=logging.INFO,
    format="%(asctime)s - %(message)s"
)


# Function to detect unmapped columns
def detect_unmapped_columns(df: pd.DataFrame, mapping: dict) -> None:
    # Get all mapped columns (flatten the mapping values)
    mapped_columns = set(col for variations in mapping.values() for col in variations)

    # Identify unmapped columns
    unmapped_columns = set(df.columns) - mapped_columns

    if unmapped_columns:
        logging.info("Unmapped columns detected: %s", list(unmapped_columns))
        for col in list(unmapped_columns):
            field = input(
                f"Unmapped column '{col}' detected. Map to which field in the schema? (Leave blank to ignore): ")
            if field:
                mapping.setdefault(field, []).append(col)


# Function to preprocess data
def preprocess_data(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    # Detect unmapped columns
    detect_unmapped_columns(df, mapping)

    # Translate columns to common schema
    rename_map = {}
    for standard_col, variations in mapping.items():
        for col in variations:
            if col in df.columns:
                logging.info(f'renaming {col} to {standard_col}')
                rename_map[col] = standard_col
                break
    df = df.rename(columns=rename_map)
    return df


# Function to translate data
def translate_to_common_schema(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    rename_map = {}
    for standard_col, variations in mapping.items():
        for col in variations:
            if col in df.columns:
                rename_map[col] = standard_col
                break
    df = df.rename(columns=rename_map)
    return ensure_all_columns(df, data_maps_schema)


# Ensure all columns exist
def ensure_all_columns(df: pd.DataFrame, schema: list) -> pd.DataFrame:
    for col in schema:
        if col not in df.columns:
            df[col] = None
    return df


def create_datamaps_instances(df: pd.DataFrame) -> list:
    return [DataMaps(**row.to_dict()) for _, row in df.iterrows()]


if __name__ == "__main__":
    with open('tests/mkt_orders_raw.json', 'r') as f:
        orders = json.load(f)

    df = aggregate_sell_orders(orders)

    processed_data = preprocess_data(df, column_mapping)
    standardized_data = translate_to_common_schema(df, column_mapping)

    variations = column_mapping["type_name"]

    if "type_name" not in standardized_data.columns or standardized_data["type_name"].isnull().all():
        print("The 'type_name' column is missing or all its values are None.")
        named_data = insert_pd_type_names(standardized_data)

    pd.set_option('display.max_columns', None)
    try:
        print(named_data.head())
    except:
        print("error")
        #
    #
    # data_maps_instances = create_datamaps_instances(standardized_data)
    #
    # for instance in data_maps_instances:
    #     print(instance.type_id)
