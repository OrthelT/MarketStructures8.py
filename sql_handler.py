import logging
from datetime import datetime, timezone
from typing import List

import pandas as pd
from sqlalchemy import (create_engine, text)
from sqlalchemy.orm import declarative_base

from doctrine_monitor import read_doctrine_watchlist

sql_logger = logging.getLogger('mkt_structures.sql_handler')

sql_file = "market_orders.sqlite"
mkt_sqlfile = "sqlite:///market_orders.sqlite"
fit_sqlfile = "Orthel:Dawson007!27608@localhost:3306/wc_fitting"
fit_mysqlfile = "mysql+pymysql://Orthel:Dawson007!27608@localhost:3306/wc_fitting"

Base = declarative_base()

market_columns = [
    "type_id",
    "volume_remain",
    "price",
    "issued",
    "duration",
    "order_id",
    "is_buy_order",
]
history_columns = [
    "date",
    "type_id",
    "average",
    "highest",
    "lowest",
    "order_count",
    "volume",
]
stats_columns = [
    'type_id',
    'total_volume_remain',
    'min_price',
    'price_5th_percentile',
    'avg_of_avg_price',
    'avg_daily_volume',
    'group_id',
    'type_name',
    'group_name',
    'category_id',
    'days_remaining'
    'timestamp'
]


def create_tables(database):
    engine = create_engine(database, echo=False)
    Base.metadata.create_all(engine)

def insert_pd_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    ts = datetime.now(timezone.utc)

    # detect if timestamp already exists
    if 'timestamp' in df2.columns:
        if pd.api.types.is_datetime64_any_dtype(df2['timestamp']):
            sql_logger.info('timestamp exists')
            return df2
        else:
            df2.drop(columns=['timestamp'], inplace=True)

    df2.loc[:, "timestamp"] = ts
    return df2

def insert_pd_type_names(df: pd.DataFrame) -> pd.DataFrame:
    engine = create_engine(mkt_sqlfile, echo=False)
    ids = df['type_id'].unique().tolist()

    # Generate named placeholders dynamically
    placeholders = ",".join([f":id{i}" for i in range(len(ids))])

    # SQL query with named placeholders
    query = text(f"""
        SELECT typeID, typeName 
        FROM JoinedInvTypes
        WHERE typeID IN ({placeholders})
    """)

    # Create a dictionary of named parameters
    params = {f"id{i}": value for i, value in enumerate(ids)}

    # Execute the query with named parameters
    with engine.connect() as conn:
        result = conn.execute(query, params)
        results = result.fetchall()

    names = pd.DataFrame(results, columns=['type_id', 'type_name'])

    print(df.head())
    df.drop(columns=['type_name'], inplace=True, errors='ignore')
    df2 = df.merge(names, on='type_id', how='left')
    names = df2['type_name']
    df2.drop(columns=['type_name'], inplace=True)

    df2.insert(1, 'type_name', names)
    print(df2.head())
    return df2

def process_pd_dataframe(
        df: pd.DataFrame, columns: list, date_column: str = None
) -> pd.DataFrame:
    """Process the dataframe by selecting columns and converting dates."""
    # Select only the specified columns
    df = df[columns]

    # Determine the date column if not provided
    if date_column is None:
        if "date" in df.columns:
            date_column = "date"
        elif "issued" in df.columns:
            date_column = "issued"
        else:
            date_column = None

    # Convert date strings to datetime objects
    if date_column:
        df.loc[:, date_column] = pd.to_datetime(df[date_column], errors='coerce')

    # Insert timestamp
    df = insert_pd_timestamp(df)

    return df

def process_esi_market_order_optimized(data: List[dict], is_history: bool = False) -> str:
    # Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(data)
    # Database setup
    if is_history:

        try:
            status = update_history(df)
            # Bulk insert using Pandas
        except Exception as e:
            print(f"Error updating history data: {str(e)}")
            status = f"Error updating history data: {str(e)}"
            raise

    # Skip type name update and current order update if history is True
    else:
        try:
            status = update_orders(df)
        except Exception as e:
            sql_logger.warning(f"Error updating orders: {str(e)}")
            status = f"Error updating orders: {str(e)}"
            raise
    sql_logger.info(print("{status} Doctrine items loading completed successfully!"))
    return status

def read_history(doys: int = 30) -> pd.DataFrame:
    engine = create_engine(mkt_sqlfile, echo=False)

    # Create a session factoryf
    session = engine.connect()
    sql_logger.info(f'connection established: {session} by sql_handler.read_history()')
    d = f"'-{doys} days'"

    stmt = f"""
    SELECT * FROM market_history
    WHERE date >= date('now', {d})"""

    historydf = pd.read_sql(stmt, session)
    session.close()
    sql_logger.info(f'connection closed: {session}...returning orders from market_history table.')
    return historydf

def update_history(df: pd.DataFrame) -> str:
    engine = create_engine(mkt_sqlfile, echo=False)

    optimize_for_bulk_update(engine)

    with engine.connect() as con:
        try:
            df_processed = process_pd_dataframe(df, history_columns)
            status = "data processed"
        except Exception as e:
            sql_logger.error(print(f'an exception occurred in process_pd_dataframe(df, history_columns): {e}'))
            raise
        try:
            df_named = insert_pd_type_names(df_processed)
            status += ", type names updated"
        except Exception as e:
            sql_logger.error(print(f'an exception occurred in insert_pd_type_names(df_processed): {e}'))
            raise
        try:
            df_named.to_sql('market_history', con=engine, if_exists='replace', index=False, chunksize=1000)
            status += ", data loaded"
        except Exception as e:
            sql_logger.error(print(f'an exception occurred in df_processed.to_sql: {e}'))
            raise

    revert_sqlite_settings(engine)

    return status

def update_orders(df: pd.DataFrame) -> str:
    sql_logger.info("updating orders...initiating engine")
    engine = create_engine(mkt_sqlfile, echo=False)

    sql_logger.info("updating orders...optimizing for bulk update")
    optimize_for_bulk_update(engine)

    with engine.connect() as con:
        try:
            df_processed = process_pd_dataframe(df, market_columns)
            status = "data processed"
        except Exception as e:
            sql_logger.error(print(f'an exception occurred in process_pd_dataframe(df, history_columns): {e}'))
            raise
        try:
            df_named = insert_pd_type_names(df_processed)
            status += ", type names updated"
        except Exception as e:
            sql_logger.error(print(f'an exception occurred in insert_pd_type_names(df_processed): {e}'))
            raise
        try:
            df_named.to_sql('market_order', con=engine, if_exists='replace', index=False, chunksize=1000)
            status += ", data loaded"
        except Exception as e:
            sql_logger.error(print(f'an exception occurred in df_processed.to_sql: {e}'))
            raise

    revert_sqlite_settings(engine)

    return status

def update_stats(df: pd.DataFrame) -> str:
    df = df.infer_objects()
    df = df.fillna(0)

    df_processed = insert_pd_timestamp(df)

    # start a session
    engine = create_engine(mkt_sqlfile, echo=False)
    try:
        # clear existing table
        with engine.connect() as conn:

            conn.execute(text("DELETE FROM Market_Stats"))
            conn.commit()
        sql_logger.info("Market_Stats table cleared")

        df_processed.to_sql('Market_Stats', engine,
                            if_exists='replace',
                            index=False,
                            method='multi',
                            chunksize=1000)
        status = "Data loading completed successfully!"
        return status
    except Exception as e:
        sql_logger.error(f"Error occurred: {str(e)}")
        raise

def optimize_for_bulk_update(engine):
    # Optimize database settings for bulk insert
    sql_logger.info('optimizing for bulk update')
    with engine.begin() as conn:
        conn.execute(text("PRAGMA synchronous = OFF;"))
        conn.execute(text("PRAGMA journal_mode = MEMORY;"))

def revert_sqlite_settings(engine):
    # Revert SQLite to safer defaults
    with engine.begin() as conn:
        conn.execute(text("PRAGMA synchronous = FULL;"))
        conn.execute(text("PRAGMA journal_mode = DELETE;"))

def read_sql_watchlist() -> pd.DataFrame:
    # grabs the current watchlist and returns it as a dataframe
    engine = create_engine(mkt_sqlfile, echo=False)
    with engine.connect() as conn:
        df = pd.read_sql_table('watchlist_mkt', conn)
    df = df.rename(
        columns={
            "typeID": "type_id",
        })
    # check to make sure there are not any doctrine items not included in the
    # current watchlist
    doc = read_doctrine_watchlist()

    missing = doc[~doc['type_id'].isin(df['type_id'])]

    if missing.empty:
        sql_logger.info("no missing items found, returning watchlist")
    else:
        sql_logger.info("missing items found, merging doctrine_ids into watchlist")
        df = pd.concat([df, missing])
        df.reset_index(inplace=True, drop=True)
    df.drop(columns=['categoryID_2', 'metaGroupID_2'], inplace=True)

    return df

def read_sql_market_stats() -> pd.DataFrame:
    engine = create_engine(mkt_sqlfile, echo=False)
    with engine.connect() as conn:
        df = pd.read_sql_table('Market_Stats', conn)
    return df

def fill_missing_stats_v2(df) -> pd.DataFrame:
    sql_logger.info('checking missing stats...starting')
    stats = df
    watchlist = read_sql_watchlist()

    stats['type_id'] = stats['type_id'].astype(int)

    missing = watchlist[~watchlist['type_id'].isin(stats['type_id'])]
    missing.reset_index(inplace=True, drop=True)
    print(f'found missing items: {len(missing)}. Filling from history data.')
    missing_df = pd.DataFrame(
        columns=['type_id', 'total_volume_remain', 'min_price', 'price_5th_percentile',
                 'avg_of_avg_price', 'avg_daily_volume', 'group_id', 'type_name',
                 'group_name', 'category_id', 'category_name', 'days_remaining', 'timestamp'])
    missing_df = pd.concat([missing, missing_df])
    missing_df['total_volume_remain'] = 0

    # fill historical values where available
    hist = read_history(30)
    hist_grouped = hist.groupby("type_id").agg({'average': 'mean', 'volume': 'mean'})
    missing_df['avg_of_avg_price'] = missing_df['type_id'].map(hist_grouped['average'])
    missing_df['avg_daily_volume'] = missing_df['type_id'].map(hist_grouped['volume'])

    # all null values must die
    missing_df = missing_df.infer_objects()
    missing_df = missing_df.fillna(0)
    sql_logger.info('missing stats updated')
    return missing_df


if __name__ == "__main__":
    pass