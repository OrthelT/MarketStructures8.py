import logging

import pandas as pd
import sqlalchemy
from matplotlib import pyplot as plt
from sqlalchemy import create_engine, exc

from doctrine_monitor import get_doctrine_fits, get_fit_items

shared_logger = logging.getLogger('mkt_structures.shared_utils')
mkt_sqldb = "sqlite:///market_orders.sqlite"
fit_mysqldb = "mysql+pymysql://Orthel:Dawson007!27608@localhost:3306/wc_fitting"
mkt_sqlfile = mkt_sqldb

def get_doctrine_status_optimized(watchlist, target: int = 20) -> pd.DataFrame:
    fits_df = get_doctrine_fits(db_name='wc_fitting')
    target_items = get_fit_items(fits_df)

    engine = create_engine(mkt_sqldb, echo=False)
    with engine.connect() as conn:
        market_stats = pd.read_sql_table('Market_Stats', conn)

    market_stats['type_id'] = market_stats['type_id'].astype(int)

    target_df = target_items.merge(market_stats, on='type_id', how='left')
    target_df.drop(columns=['type_name_y'], inplace=True)
    target_df.rename(columns={'type_name_x': 'type_name', 'doctrine_name': 'fit_name'}, inplace=True)

    target_df['fits_on_market'] = target_df['total_volume_remain'] / target_df['quantity']
    target_df['fits_on_market'] = target_df['fits_on_market'].round(0)
    target_df['delta'] = target_df['fits_on_market'] - target

    engine = create_engine(fit_mysqldb)

    with engine.connect() as conn:
        df1 = pd.read_sql_table('fittings_doctrine_fittings', conn)
        df2 = pd.read_sql_table('fittings_doctrine', conn)

    df1.rename(columns={'fitting_id': 'fit_id'}, inplace=True)
    df2.rename(columns={'id': 'doctrine_id'}, inplace=True)

    df3 = target_df.merge(df1, on='fit_id', how='left')
    df4 = df3.merge(df2, on='doctrine_id', how='left')

    df = df4.copy()

    drop_cols = ['icon_url', 'description', 'created', 'last_updated', 'id', 'min_price']
    df.drop(columns=drop_cols, inplace=True)
    df.reset_index(drop=True, inplace=True)

    renamed_cols = {'name': 'doctrine', 'ship_type_name': 'ship', 'ship_type_id': 'ship id', 'fit_name': 'fit',
                    'total_volume_remain': 'stock', 'price_5th_percentile': '4H price',
                    'avg_of_avg_price': 'avg price', 'avg_daily_volume': 'avg vol', 'group_id': 'grp id',
                    'group_name': 'group', 'category_id': 'cat id', 'category_name': 'category',
                    'days_remaining': 'days', 'fits_on_market': 'fits', 'doctrine_id': 'doc id',
                    'type_name': 'item', 'fit_id': 'fit id', 'type_id': 'type id', 'quantity': 'qty'}

    df.rename(columns=renamed_cols, inplace=True)

    reordered_cols = ['fit id', 'type id', 'category', 'fit', 'ship', 'item', 'qty', 'stock', 'fits',
                      'days', '4H price', 'avg vol', 'avg price', 'delta', 'doctrine', 'group', 'cat id',
                      'grp id', 'doc id', 'ship id', 'timestamp']

    df = df[reordered_cols]
    df.infer_objects()
    df.fillna(0, inplace=True)
    return df

def read_doctrine_watchlist() -> pd.DataFrame:
    shared_logger.info('reading doctrine watchlist')
    try:
        # Create the connection string without quotes around database name
        mysql_connection = fit_mysqldb
        # Create engine with echo=True to see SQL output for debugging
        engine = sqlalchemy.create_engine(mysql_connection, echo=False)
        shared_logger.info('MySql db connection established')
        shared_logger.info('accessing doctrines...')

        # Test the connection before executing query
        with engine.connect() as connection:
            # Your SQL query
            query = """
            SELECT DISTINCT t.type_id
            FROM watch_doctrines as w
            JOIN fittings_doctrine_fittings f on w.id = f.doctrine_id
            JOIN fittings_fittingitem t on f.fitting_id = t.fit_id
            JOIN fittings_type ft on t.type_id = ft.type_id
            """
            # Execute query and convert to DataFrame
            df = pd.read_sql_query(query, connection)
            print(f"""
            ===============
            Doctrine watchlist retrieved: {len(df)} items
          

            """)
        # merge in type info for compatability with Mkt Sql file
        engine = create_engine(mkt_sqldb)
        with engine.connect() as conn:
            type_info = pd.read_sql_table('JoinedInvTypes', conn)

    except exc.OperationalError as e:
        shared_logger.error(f"Database connection error: {str(e)}")
    except exc.ProgrammingError as e:
        shared_logger.error(f"SQL query error: {str(e)}")
    except Exception as e:
        shared_logger.error(f"Unexpected error: {str(e)}")

    old_cols = ['typeID', 'groupID', 'typeName', 'groupName', 'categoryID',
                'categoryName']
    drop_cols = ['metaGroupID',
                 'metaGroupName']
    new_cols = ['type_id', 'group_id', 'type_name', 'group_name', 'category_id',
                'category_name']

    type_info.drop(columns=drop_cols, inplace=True)
    type_info.rename(columns=dict(zip(old_cols, new_cols)), inplace=True)

    df2 = pd.merge(df, type_info, on='type_id', how='left').reset_index(drop=True)
    df2.infer_objects()

    return df2

def fill_missing_stats_v2(df: pd.DataFrame, watchlist: pd.DataFrame) -> pd.DataFrame:
    shared_logger.info('checking missing stats...starting')
    stats = df

    if 'type id' in stats.columns:
        stats.rename(columns={'type id': 'type_id'}, inplace=True)

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
    engine = create_engine(mkt_sqldb, echo=False)
    days_hist = 30
    # Create a session factory
    session = engine.connect()
    shared_logger.info(f'connection established: {session} by sql_handler.read_history()')
    d = f"'-{days_hist} days'"

    stmt = f"""
    SELECT * FROM market_history
    WHERE date >= date('now', {d})"""

    history_data = pd.read_sql(stmt, session)
    session.close()
    shared_logger.info(f'connection closed: {session}...returning orders from market_history table.')

    hist_grouped = history_data.groupby("type_id").agg({'average': 'mean', 'volume': 'mean'})
    missing_df['avg_of_avg_price'] = missing_df['type_id'].map(hist_grouped['average'])
    missing_df['avg_daily_volume'] = missing_df['type_id'].map(hist_grouped['volume'])

    # all null values must die
    missing_df = missing_df.infer_objects()
    missing_df = missing_df.fillna(0)
    shared_logger.info('missing stats updated')
    updated_df = pd.concat([stats, missing_df])
    updated_df = updated_df.infer_objects()

    de_duped_df = updated_df.drop_duplicates()

    return de_duped_df


def read_full_history() -> pd.DataFrame:
    engine = create_engine(mkt_sqldb, echo=False)
    with engine.connect() as conn:
        df = pd.read_sql_table('full_market_history', conn)
    return df


def load_full_history() -> None:
    df = read_market_orders()
    ids = df.type_id.unique().tolist()
    pd.set_option('display.max_columns', None)
    df2, history_dict = fetch_market_history(True, id_list=ids)

    engine = create_engine(mkt_sqlfile, echo=False)
    with engine.connect() as conn:
        df2.to_sql('full_market_history', conn, if_exists='replace', index=False)
    print('full history loaded')


def get_30_days_trade_volume() -> pd.DataFrame:
    df = read_full_history()
    df['isk_volume'] = df['volume'] * df['average']
    df2 = df.groupby('date').agg({'isk_volume': 'sum'}).reset_index()
    df2['date'] = pd.to_datetime(df2['date'])
    # df2.set_index('date', inplace=True)
    last_thirty_days = df2.tail(30)
    total_isk = last_thirty_days['isk_volume'].sum()
    print(f'total isk volume: {total_isk}')
    return last_thirty_days


def plot_30_days_trade_volume() -> None:
    df = get_30_days_trade_volume()
    plt.figure(figsize=(12, 6))
    plt.bar(df['date'], df['isk_volume'])
    plt.title('30-day trade volume')
    plt.xlabel('Date')
    plt.ylabel('Total ISK volume')
    plt.show()


def get_doctrine_mkt_status() -> pd.DataFrame:
    mysql_connection = fit_mysqldb  # Assuming this variable is defined elsewhere in your code
    # Create engine with echo=False to hide SQL output
    engine = sqlalchemy.create_engine(mysql_connection, echo=False)

    # First query: Get doctrine and fitting IDs
    with engine.connect() as connection:
        query = """
                SELECT fdf.doctrine_id, fdf.fitting_id
                FROM watch_doctrines as w
                JOIN fittings_doctrine_fittings fdf on w.id = fdf.doctrine_id
                """
        df = pd.read_sql_query(query, connection)

    # Process the results
    df_fittings = df.drop_duplicates(subset=['fitting_id'])
    fit_ids = df_fittings['fitting_id'].unique().tolist()

    # Second query: Get fitting items
    if fit_ids:
        # Format the IDs for the IN clause
        fit_ids_str = ', '.join(str(id) for id in fit_ids)
        query = f"SELECT * FROM fittings_fittingitem WHERE fit_id IN ({fit_ids_str})"
        # Reuse the existing engine instead of creating a new one
        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn)
    else:
        # Handle empty list case
        df = pd.DataFrame()

    df2 = df.drop(columns=["id", "flag", "type_fk_id"])
    print(df2.columns)

    df2 = df2[["fit_id", "type_id", "quantity"]]

    df3 = df2.groupby(["fit_id", "type_id"]).sum().reset_index()
    print(f"df3.columns: {df3.columns}")
    type_ids = df3['type_id'].unique().tolist()
    type_ids_str = ', '.join(str(id) for id in type_ids)
    query = f"SELECT * FROM Market_Stats WHERE Market_Stats.type_id IN ({type_ids_str})"

    engine = create_engine(mkt_sqldb)
    with engine.connect() as conn:
        df_ms = pd.read_sql_query(query, conn)

    doctrines = df3.merge(df_ms, on='type_id', how='left')
    pd.set_option('display.max_columns', None)

    doctrines["fits"] = doctrines["total_volume_remain"] / doctrines["quantity"]
    doctrines["fits"] = doctrines["fits"].round(0)

    fit_ids = doctrines['fit_id'].unique().tolist()
    fit_ids_str = ', '.join(str(fit_id) for fit_id in fit_ids)

    query3 = f"""
    SELECT id,ship_type_id FROM fittings_fitting 
    WHERE id IN ({fit_ids_str})
    """

    engine = create_engine(fit_mysqldb, echo=False)
    with engine.connect() as conn:
        df_ship_types = pd.read_sql_query(query3, conn)

    df_ship_types.rename(columns={'id': 'fit_id', 'ship_type_id': 'ship_id'}, inplace=True)

    doctrines = doctrines.merge(df_ship_types, on='fit_id', how='left')

    ship_ids = doctrines['ship_id'].unique().tolist()

    ship_ids_str = ",".join(str(id) for id in ship_ids)

    engine = create_engine(mkt_sqldb)

    query = f"""
        SELECT typeID, typeName from JoinedInvTypes where typeID in ({ship_ids_str})
    """

    df_ship_names = pd.read_sql_query(query.format(ship_ids_str=ship_ids_str), engine)
    df_ship_names.rename(columns={'typeID': 'ship_id', 'typeName': 'ship_name'}, inplace=True)

    doctrines = doctrines.merge(df_ship_names, on='ship_id', how='left')

    ship_ids = doctrines["ship_id"].unique().tolist()
    ship_ids_str = ', '.join(str(id) for id in ship_ids)
    query = f"SELECT type_id, total_volume_remain FROM Market_Stats WHERE Market_Stats.type_id IN ({ship_ids_str})"

    engine = create_engine(mkt_sqldb)
    with engine.connect() as conn:
        df_sms = pd.read_sql_query(query, conn)

    df_sms.rename(columns={'type_id': 'ship_id', 'total_volume_remain': 'hulls'}, inplace=True)

    doctrines = doctrines.merge(df_sms, on='ship_id', how='left')

    drops = ["min_price", 'avg_of_avg_price']
    doctrines.drop(columns=drops, inplace=True)
    rename_cols = {
        'total_volume_remain': 'total_stock',
        'quantity': 'fit_qty',
        'price_5th_percentile': '4H_price',
        'avg_daily_volume': 'avg_vol',
        'days_remaining': 'days',
        'fits': 'fits_on_mkt'

    }
    doctrines.rename(columns=rename_cols, inplace=True)

    new_col_order = ['fit_id', 'ship_id', 'ship_name', 'hulls', 'type_id', 'type_name', 'fit_qty',
                     'fits_on_mkt', 'total_stock', '4H_price', 'avg_vol', 'days',
                     'group_id', 'group_name', 'category_id', 'category_name', 'timestamp']

    doctrines = doctrines[new_col_order]
    doctrines.infer_objects()
    doctrines.fillna(0, inplace=True)
    doctrines[["hulls", "total_stock", "group_id", "category_id"]] = doctrines[
        ["hulls", "total_stock", "group_id", "category_id"]].astype(int)
    doctrines.fits_on_mkt = doctrines.fits_on_mkt.round(0)
    doctrines["4H_price"] = doctrines["4H_price"].round(0)
    doctrines.avg_vol = doctrines.avg_vol.round(0)
    doctrines.days = doctrines.days.round(0)

    return doctrines


def find_missing_ships(df):
    ids = df['type_id'].unique().tolist()
    ship_ids = df['ship_id'].unique().tolist()

    s_ids = [id for id in ids if id in ship_ids]
    print(s_ids)


if __name__ == '__main__':
    pass