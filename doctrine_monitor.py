
import logging

import pandas as pd
import sqlalchemy

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

    df = df[~df['name'].str.startswith("zz ")].reset_index(drop=True)

    return df


def get_fit_items(df: pd.DataFrame):
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
    df3 = df3.reset_index(drop=True)
    print("check 1: fit_id = 492")
    print(df3[df3.fit_id == 492])

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
    fit_items.type_id = fit_items.type_id.astype(int)
    return fit_items




if __name__ == "__main__":
    pass
