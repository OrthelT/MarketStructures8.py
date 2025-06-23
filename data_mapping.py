from logging_tool import configure_logging

import pandas as pd

from models import DataMaps

# Example mapping
column_mapping = {
    "type_id": ["type_id", "typeID", "type id", "typeId"],
    "type_name": ["type_name", "typeName", "item"],
    "stock": ["stock", "volume_remain", "total_volume_remain", "qty on mkt"],
    "min_price": ["min_price", "min price"],
    "price": ["price", "4H price", "price_5th_percentile"],
    "avg_price": ["avg_price", "avg_of_avg_price", 'avg price'],
    "avg_volume": ["avg_volume", "avg daily volume", "avg_daily_volume", 'avg vol'],
    "group": ["group", "group_name", 'groupName'],
    "group_id": ["group_id", "grp id", 'groupID'],
    "category": ["category", "category_name", "categoryName"],
    "category_id": ["category_id", "cat id", "categoryID", "cat_id"],
    "days": ["days", "days_remaining"],
    "timestamp": ["timestamp", "issued"],
    "fits": ["fits", "qty_on_market"],
    "fit": ["fit", "fit_name", "fit name", "name"],
    "fit_id": ["fit_id", "fit id", "id"],
    "doctrine": ["doctrine", "doctrine_name"],
    "doctrine_id": ["doctrine_id", "doc id"],
    "ship": ["ship", "ship_name", "ship name"],
    "ship_id": ["ship_id", "hull_id", "hull id", "ship id", "ship_type_id"],
    "qty": ["qty", "quantity", "qty_required"],
    "metagroup_id": ["metagroup_id", "metaGroupID"],
    "meta_name": ["meta_name", "metaGroupName"]


}

# Target schema (all columns needed in DataMaps)
data_maps_schema = [
    "type_id", "type_name", "group_id", "category_id",
    "price", "avg_price", "avg_volume", "stock", "days",
    "fits", "fit", "fit_id", "doctrine", "doctrine_id",
    "ship", "ship_id", "qty", "timestamp"
]

# Configure logging
logger = configure_logging(log_name=__name__)

# Function to detect unmapped columns
def detect_unmapped_columns(df: pd.DataFrame, mapping: dict) -> None:
    # Get all mapped columns (flatten the mapping values)
    mapped_columns = set(col for variations in mapping.values() for col in variations)

    # Identify unmapped columns
    unmapped_columns = set(df.columns) - mapped_columns

    if unmapped_columns:
        logger.info("Unmapped columns detected: %s", list(unmapped_columns))
        for col in list(unmapped_columns):
            field = input(
                f"Unmapped column '{col}' detected. Map to which field in the schema? (Leave blank to ignore): ")
            if field:
                mapping.setdefault(field, []).append(col)


# Function to preprocess data
def preprocess_data(df: pd.DataFrame, mapping=None) -> pd.DataFrame:
    # Detect unmapped columns
    if mapping is None:
        mapping = column_mapping
    detect_unmapped_columns(df, mapping)

    # Translate columns to common schema
    rename_map = {}
    reverse_map = {}
    for standard_col, variations in mapping.items():
        for col in variations:
            if col in df.columns:
                logger.info(f'renaming {col} to {standard_col}')
                rename_map[col] = standard_col
                reverse_map[standard_col] = col
                print(f'renamed {col} to {standard_col}')
                break
    df = df.rename(columns=rename_map)
    return df


# Function to translate data
def translate_to_common_schema(df: pd.DataFrame, mapping=None) -> pd.DataFrame | dict:
    if mapping is None:
        mapping = column_mapping
    rename_map = {}
    for standard_col, variations in mapping.items():
        for col in variations:
            if col in df.columns:
                rename_map[col] = standard_col
                break
    df = df.rename(columns=rename_map)

    return df


# Ensure all columns exist
def ensure_all_columns(df: pd.DataFrame, schema: list) -> pd.DataFrame:
    for col in schema:
        if col not in df.columns:
            df[col] = None
    return df


def create_datamaps_instances(df: pd.DataFrame) -> list:
    return [DataMaps(**row.to_dict()) for _, row in df.iterrows()]


def map_data(df: pd.DataFrame) -> pd.DataFrame:
    df = preprocess_data(df)
    df = translate_to_common_schema(df)
    return df


def remap_reversable(df: pd.DataFrame) -> pd.DataFrame | dict:
    nonstandard_values = []
    rename_mappings = {}
    reverse_mappings = {}

    for col in df.columns:
        if col not in column_mapping.keys():
            nonstandard_values.append(col)

    for col in nonstandard_values:
        for key, value in column_mapping.items():
            if col in value:
                rename_mappings[col] = key
                reverse_mappings[key] = col
    for key, value in rename_mappings.items():
        print(f'renamed {key} to {value}')
    df = df.rename(columns=rename_mappings)
    return df, reverse_mappings


def reverse_remap(df: pd.DataFrame, reverse_mappings: dict) -> pd.DataFrame:
    df = df.rename(columns=reverse_mappings)
    return df

if __name__ == "__main__":
    pass
