import logging
from datetime import datetime, timezone
from typing import List

import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.ticker import FuncFormatter
from sqlalchemy import create_engine, text
from sqlalchemy.orm import declarative_base

from data_mapping import remap_reversable, reverse_remap
from shared_utils import read_doctrine_watchlist, get_doctrine_status_optimized

sql_logger = logging.getLogger('mkt_structures.sql_handler')
brazil_logger = logging.getLogger('mkt_structures.brazil_logger')
brazil_logger.addHandler(logging.FileHandler('logs/brazil_logger.log'))
brazil_logger.setLevel(logging.INFO)

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


def get_item_history(item_id: int, doys: int = 30) -> pd.DataFrame:
    engine = create_engine(mkt_sqlfile, echo=False)

    # Create a session factoryf
    session = engine.connect()
    sql_logger.info(f'connection established: {session} by sql_handler.read_history()')
    d = f"'-{doys} days'"

    stmt = f"""
    SELECT * FROM market_history
    WHERE date >= date('now', {d}) AND type_id = {item_id}"""

    item_historydf = pd.read_sql(stmt, session)
    session.close()
    sql_logger.info(f'connection closed: {session}...returning orders from market_history table.')

    return item_historydf

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
    print(f'missing items = {len(missing)}, {missing.type_id.unique().tolist()}')
    return df

def read_sql_market_stats() -> pd.DataFrame:
    engine = create_engine(mkt_sqlfile, echo=False)
    with engine.connect() as conn:
        df = pd.read_sql_table('Market_Stats', conn)
    return df


def read_sql_mkt_orders() -> pd.DataFrame:
    engine = create_engine(mkt_sqlfile, echo=False)
    with engine.connect() as conn:
        df = pd.read_sql_table('market_order', conn)
    return df

def update_doctrine_stats():
    watchlist = read_sql_watchlist()
    df = get_doctrine_status_optimized(watchlist)

    engine = create_engine(mkt_sqlfile)

    reordered_cols = ['fit id', 'type id', 'category', 'fit', 'ship', 'item', 'qty', 'stock', 'fits',
                      'days', '4H price', 'avg vol', 'avg price', 'delta', 'doctrine', 'group', 'cat id',
                      'grp id', 'doc id', 'ship id', 'timestamp']

    cols = ['fit_id', 'type_id', 'category', 'fit', 'ship', 'item', 'qty', 'stock', 'fits', 'days', 'price_4h',
            'avg_vol', 'avg_price', 'delta', 'doctrine', 'group', 'cat_id', 'grp_id', 'doc_id', 'ship_id', 'timestamp']

    colszip = zip(reordered_cols, cols)
    df.rename(columns=dict(colszip), inplace=True)
    df['timestamp'] = datetime.now(timezone.utc)
    df.infer_objects()
    df.fillna(0, inplace=True)

    with engine.connect() as conn:
        status = df.to_sql('Doctrines', conn, if_exists='replace', index=False)
    print(f'database update completed for {status} doctrine items')

def add_fit_to_watchlist(fit) -> None:
    df = read_sql_watchlist()
    missing = fit[~fit['type_id'].isin(df['type_id'])]

    df2, reverse_map = remap_reversable(df)

    miss_col = missing.columns.tolist()
    df2_col = df2.columns.tolist()
    miss_drop = []

    for col in miss_col:
        if col not in df2_col:
            miss_drop.append(col)

    missing.drop(columns=miss_drop, inplace=True)

    df2 = pd.concat([df2, missing])

    df3 = reverse_remap(df2, reverse_map)

    print(df3.head())
    print(f'{len(df)}->{len(df3)}')

    check_db_ready = input('ready to update? (y/n) ')

    if check_db_ready == 'y':
        engine = create_engine(mkt_sqlfile)
        with engine.connect() as conn:
            df2.to_sql('watchlist_mkt', conn, if_exists='replace', index=False)
        print(f'database update completed for {len(missing)} missing items')
    else:
        print("exiting without updating database")
        return None


def market_data_to_brazil():
    stats = read_sql_market_stats()
    stats.rename(columns={'timestamp': 'last_update'}, inplace=True)

    renaming_dict = {
        'price_5th_percentile': 'price',
        'avg_of_avg_price': 'avg_price',
        'avg_daily_volume': 'avg_vol'
    }
    stats.rename(columns=renaming_dict, inplace=True)
    brazil_logger.info(f'STATS PROCESSED: {len(stats)}')

    orders = read_sql_mkt_orders()
    orders = orders.drop(columns=['timestamp'])
    brazil_logger.info(f'ORDERS PROCESSED: {len(stats)}')

    orders.to_csv('output/brazil/new_orders.csv')
    orders.to_json('output/brazil/new_orders.json')
    brazil_logger.info('orders updated')

    stats.to_csv('output/brazil/new_stats.csv')
    stats.to_json('output/brazil/new_stats.json')
    brazil_logger.info('stats updated')

    brazil_history()

    return None


def brazil_history() -> None:
    df = read_history()
    df.date = pd.to_datetime(df.date).dt.date
    df = df.drop(columns=['timestamp'])

    ids = df.type_id
    df = df.drop(columns=['type_id'])
    df.insert(1, "type_id", ids.astype(int))

    df = df.sort_values(by=['date'], ascending=False)
    df = df.reset_index(drop=True)
    df['average'] = df['average'].round(1)

    brazil_logger.info(f'processed {len(df)} lines of history data')
    df.to_csv('output/brazil/new_history.csv')
    df.to_json('output/brazil/new_history.json')
    brazil_logger.info('saved history data to csv and json')

    return None


def plot_item_history(item_id: int, days: int = 60):
    df = get_item_history(item_id=item_id, doys=days)
    df.date = pd.to_datetime(df.date, utc=True)
    print(df.dtypes)
    title = df.type_name.unique()[0]
    df2 = df[['date', 'volume']]
    df2.date = df2.date.dt.date

    df2.plot(
        x='date',
        y=['volume'],
        rot=45,
        kind='bar',
        style=['o-', 'o-'],
        title=f'{title} (volume)',
        figsize=(10, 8),
        xlabel='Date'

    )
    plt.show()


def billions_formatter(x, _):
    return f'{x / 1e9:.1f}B'


def plot_daily_total_ISK():
    df = read_history()
    ids = df.type_id.unique().tolist()
    pd.set_option('display.max_columns', None)

    engine = create_engine(mkt_sqlfile, echo=False)

    with engine.connect() as conn:
        df2 = pd.read_sql_table('JoinedInvTypes', conn)
    df3 = df2[df2['typeID'].isin(ids)]
    df3.reset_index(inplace=True, drop=True)

    df4 = df3[['typeID', 'groupName', 'categoryName']]
    renaming_dict = {'typeID': 'type_id', 'groupName': 'group_name', 'categoryName': 'category_name'}
    df4.rename(columns=renaming_dict, inplace=True)
    df = df.merge(df4, on='type_id', how='left')
    df['daily_value'] = df['volume'] * df['average']
    df.drop(columns=['highest', 'lowest', 'order_count', 'timestamp'], inplace=True)
    df.date = pd.to_datetime(df.date, utc=True).dt.date
    df7 = df.groupby(['date', 'category_name']).sum().reset_index().sort_values(by=['date'], ascending=False)
    df7.drop(columns=['type_name', 'average', 'group_name', 'type_id'], inplace=True)
    df8 = df7.groupby(['date']).sum().reset_index()
    df8.drop(columns=['category_name', 'volume'], inplace=True)

    title = 'Total ISK by Day'

    fig, ax = plt.subplots(figsize=(10, 8))
    df8.plot(
        x='date',
        y=['daily_value'],
        rot=45,
        kind='bar',
        style=['o-', 'o-'],
        title=f'{title} (ISK in Billions)',
        xlabel='date',
        ax=ax
    )
    ax.yaxis.set_major_formatter(FuncFormatter(billions_formatter))
    plt.show()


def market_totals() -> pd.DataFrame:
    df = read_history(60)
    df['total_ISK'] = df['volume'] * df['average']
    df.drop(columns=['highest', 'lowest', 'order_count', 'timestamp'], inplace=True)
    df.date = pd.to_datetime(df.date, utc=True).dt.date
    df2 = df.groupby(['date']).sum().reset_index()
    df2.drop(columns=['type_name', 'average', 'type_id'], inplace=True)
    thirty_day_total = df2['total_ISK'].sum()
    return df2


def read_market_orders() -> pd.DataFrame:
    engine = create_engine(mkt_sqlfile, echo=False)
    with engine.connect() as conn:
        df = pd.read_sql_table('market_order', conn)
    return df


def get_item_info(name) -> pd.DataFrame:
    engine = create_engine(mkt_sqlfile, echo=False)
    stmt = "SELECT * FROM main.Market_Stats WHERE Market_Stats.type_name LIKE :name"
    params = {'name': f'%{name}%'}
    engine = create_engine(mkt_sqlfile, echo=False)

    with engine.connect() as conn:
        df = pd.read_sql(stmt, conn, params=params)

    print(df.columns)
    print(df.head())

    sell_formatted = "{:,.0f}".format(df.price_5th_percentile.unique()[0])
    avg_formatted = "{:,.0f}".format(df.avg_of_avg_price.unique()[0])

    print(f'''
    type_id: {df.type_id.unique()[0]}
    type_name: {df.type_name.unique()[0]}
    -----------------------------
    stock: {df.total_volume_remain.unique()[0]}
    days_remaining: {df.days_remaining.unique()[0]}
    avg_volume: {df.avg_daily_volume.unique()[0]}
    sell_price: {sell_formatted}
    avg_price: {avg_formatted}
    -------
    timestamp: {df.timestamp.unique()[0]}
    ''')

if __name__ == "__main__":
    pass