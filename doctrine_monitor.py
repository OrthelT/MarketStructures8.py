
import logging

import pandas as pd
import sqlalchemy
from sqlalchemy import exc, create_engine

mkt_sqlfile = "sqlite:///market_orders.sqlite"
fit_mysqlfile = "mysql+pymysql://Orthel:Dawson007!27608@localhost:3306/wc_fitting"

logger = logging.getLogger('mkt_structures.doctrine_monitor')

def get_doctrine_fits(db_name: str = 'wc_fitting') -> pd.DataFrame:
    logger.info('accessing_db and getting doctrine fits table...')
    mysql_connection = f"mysql+mysqlconnector://Orthel:Dawson007!27608@localhost:3306/{db_name}"
    engine = sqlalchemy.create_engine(mysql_connection)
    logger.info('MySql db connection established')
    logger.info('accessing doctrines...')
    table_name = 'watch_doctrines'

    query = f"SELECT id, name FROM {table_name}"
    logger.info('reading doctrine fits table')
    with engine.connect() as connection:
        df = pd.read_sql_query(query, connection)

    doctrine_ids = ", ".join(map(str, df['id'].tolist()))
    logger.info('retrieving additional data from other db tables...')
    # Use the ids in the second query
    query2 = f"""
        SELECT doctrine_id, fitting_id 
        FROM fittings_doctrine_fittings 
        WHERE doctrine_id IN ({doctrine_ids})
    """
    doctrine_query = "SELECT id as doctrine_id, name as doctrine_name FROM watch_doctrines"

    with engine.connect() as connection:
        logger.info('reading fittings_doctrine_fittings')
        fittings = pd.read_sql_query(query2, connection)
        logger.info('reading watch_doctrines')
        doctrine_df = pd.read_sql_query(doctrine_query, engine)  # Get the doctrine names

    # # Merge with  existing dataframe
    logger.info('merging additional data')
    result_df = fittings.merge(doctrine_df, on='doctrine_id', how='left')
    fit_ids = ", ".join(map(str, result_df['fitting_id'].tolist()))

    query3 = f"""
        SELECT f.id, f.name, f.ship_type_id, t.type_name
        FROM fittings_fitting f
        JOIN fittings_type t ON t.type_id = f.ship_type_id
        WHERE f.id IN ({fit_ids})
    """

    with engine.connect() as conn:
        logger.info('reading fittings_fitting and joining fittings_type')
        df = pd.read_sql_query(query3, engine)
    return df

def get_fit_items(df: pd.DataFrame, id_list: list):
    cols = ['id', 'name', 'ship_type_id', 'type_name']
    df = df.rename({'id': 'fit_id', 'name': 'doctrine_name'}, axis="columns")

    engine = sqlalchemy.create_engine(fit_mysqlfile)

    query = f"""
        SELECT fit_id, type_id, quantity 
        FROM fittings_fittingitem
    """
    with engine.connect() as conn:
        df2 = pd.read_sql_query(query, conn)

    df3 = df.merge(df2, on='fit_id', how='left')
    grouped_df = df3.groupby(['fit_id', 'type_id', 'doctrine_name', 'type_name', 'ship_type_id'])[
        'quantity'].sum().reset_index()

    # Identify rows to be added
    ship_rows = grouped_df[['fit_id', 'ship_type_id', 'doctrine_name', 'type_name']].drop_duplicates()

    # Create new rows where type_id = ship_type_id
    new_rows = ship_rows.rename(columns={'ship_type_id': 'type_id'})
    new_rows['quantity'] = 1

    # Ensure all columns are populated
    new_rows['doctrine_name'] = ship_rows['doctrine_name']
    new_rows['type_name'] = ship_rows['type_name']
    new_rows['ship_type_id'] = ship_rows['ship_type_id']  # Restore ship_type_id column for reference

    # Append the new rows to the existing dataframe
    updated_df = pd.concat([grouped_df, new_rows], ignore_index=True)
    updated_df.rename(columns={'type_name': 'ship_type_name'}, inplace=True)

    fit_type_ids = ", ".join(map(str, updated_df['type_id'].tolist()))

    query2 = f"""
        SELECT type_id as type_id, type_name as type_name from fittings_type
        WHERE type_id IN ({fit_type_ids});
    """
    with engine.connect() as conn:
        fit_names = pd.read_sql_query(query2, conn)

    df4 = updated_df.merge(fit_names, on='type_id', how='left')
    fit_items = df4[['type_id', 'type_name', 'quantity', 'doctrine_name', 'ship_type_name', 'ship_type_id', 'fit_id', ]]
    return fit_items

def get_doctrine_status_optimized(target: int = 20) -> pd.DataFrame:

    fits_df = get_doctrine_fits(db_name='wc_fitting')
    fit_ids = fits_df['id'].tolist()
    target_items = get_fit_items(fits_df, fit_ids)

    engine = create_engine(mkt_sqlfile)
    with engine.connect() as conn:
        market_stats = pd.read_sql_table('Market_Stats', conn)

    market_stats['type_id'] = market_stats['type_id'].astype(int)
    target_df = target_items.merge(market_stats, on='type_id', how='left')
    target_df.drop(columns=['type_name_y'], inplace=True)
    target_df.rename(columns={'type_name_x': 'type_name', 'doctrine_name': 'fit_name'}, inplace=True)

    target_df['fits_on_market'] = target_df['total_volume_remain'] / target_df['quantity']
    target_df['fits_on_market'] = target_df['fits_on_market'].round(0)
    target_df['delta'] = target_df['fits_on_market'] - target

    engine = create_engine(fit_mysqlfile)
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

    return df

def read_doctrine_watchlist() -> tuple[list, pd.DataFrame | None]:
    logger.info('reading doctrine watchlist')
    try:
        # Create the connection string without quotes around database name
        mysql_connection = fit_mysqlfile
        # Create engine with echo=True to see SQL output for debugging
        engine = sqlalchemy.create_engine(mysql_connection, echo=True)
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
        # merge in type info for compatability with Mkt Sql file
        engine = create_engine(mkt_sqlfile)
        with engine.connect() as conn:
            type_info = pd.read_sql_table('JoinedInvTypes', conn)
        pd.set_option('display.max_columns', None)

    except exc.OperationalError as e:
        logger.error(f"Database connection error: {str(e)}")
    except exc.ProgrammingError as e:
        logger.error(f"SQL query error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")

    old_cols = ['typeID', 'groupID', 'typeName', 'groupName', 'categoryID',
                'categoryName']
    drop_cols = ['metaGroupID',
                 'metaGroupName']
    new_cols = ['type_id', 'group_id', 'type_name', 'group_name', 'category_id',
                'category_name']

    type_info.drop(columns=drop_cols, inplace=True)
    type_info.rename(columns=dict(zip(old_cols, new_cols)), inplace=True)

    df2 = pd.merge(df, type_info, on='type_id', how='left')

    df2.reset_index(drop=True, inplace=True)
    df3 = df2.infer_objects()
    # Convert to list and return
    id_list = df3['type_id'].tolist()
    #
    return id_list, df3


def update_doctrine_stats():
    df = get_doctrine_status_optimized()
    engine = create_engine(mkt_sqlfile)

    reordered_cols = ['fit id', 'type id', 'category', 'fit', 'ship', 'item', 'qty', 'stock', 'fits',
                      'days', '4H price', 'avg vol', 'avg price', 'delta', 'doctrine', 'group', 'cat id',
                      'grp id', 'doc id', 'ship id', 'timestamp']
    cols = ['fit_id', 'type_id', 'category', 'fit', 'ship', 'item', 'qty', 'stock', 'fits', 'days', 'price_4h',
            'avg_vol', 'avg_price', 'delta', 'doctrine', 'group', 'cat_id', 'grp_id', 'doc_id', 'ship_id', 'timestamp']

    colszip = zip(reordered_cols, cols)
    df2 = df.rename(columns=dict(colszip))

    with engine.connect() as conn:
        status = df2.to_sql('Doctrines', conn, if_exists='replace', index=False)
    print(f'database update completed for {status} doctrine items')

if __name__ == "__main__":
    pass