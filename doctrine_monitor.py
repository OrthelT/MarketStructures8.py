
import pandas as pd
import sqlalchemy
from altair import to_csv
from sqlalchemy import exc, Engine, engine, create_engine
from tornado.gen import Return

mkt_sqlfile = "sqlite:///market_orders.sqlite"
fit_mysqlfile = "mysql+pymysql://Orthel:Dawson007!27608@localhost:3306/wc_fitting"


def get_doctrine_fits(db_name: str = 'wc_fitting') -> pd.DataFrame:
    print('accessing_db')
    print('------------------------')
    mysql_connection = f"mysql+mysqlconnector://Orthel:Dawson007!27608@localhost:3306/{db_name}"
    engine = sqlalchemy.create_engine(mysql_connection)
    print('MySql db connection established')
    print('accessing doctrines...')
    table_name = 'watch_doctrines'

    query = f"SELECT id, name FROM {table_name}"

    with engine.connect() as connection:
        df = pd.read_sql_query(query, connection)

    doctrine_ids = ", ".join(map(str, df['id'].tolist()))

    # Use the ids in the second query
    query2 = f"""
        SELECT doctrine_id, fitting_id 
        FROM fittings_doctrine_fittings 
        WHERE doctrine_id IN ({doctrine_ids})
    """
    doctrine_query = "SELECT id as doctrine_id, name as doctrine_name FROM watch_doctrines"

    with engine.connect() as connection:
        fittings = pd.read_sql_query(query2, connection)
        doctrine_df = pd.read_sql_query(doctrine_query, engine)  # Get the doctrine names

    # # Merge with  existing dataframe
    result_df = fittings.merge(doctrine_df, on='doctrine_id', how='left')
    fit_ids = ", ".join(map(str, result_df['fitting_id'].tolist()))

    query3 = f"""
        SELECT f.id, f.name, f.ship_type_id, t.type_name
        FROM fittings_fitting f
        JOIN fittings_type t ON t.type_id = f.ship_type_id
        WHERE f.id IN ({fit_ids})
    """

    with engine.connect() as conn:
        df = pd.read_sql_query(query3, engine)
    return df


def get_fit_items(df: pd.DataFrame, id_list: list):
    cols = ['id', 'name', 'ship_type_id', 'type_name']
    df = df.rename({'id': 'fit_id', 'name': 'doctrine_name'}, axis="columns")

    mysql_connection = f"mysql+mysqlconnector://Orthel:Dawson007!27608@localhost:3306/wc_fitting"
    engine = sqlalchemy.create_engine(mysql_connection)

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


# def get_market_stats(doctrine_df: pd.DataFrame, orders: pd.DataFrame, target: int = 20) -> pd.DataFrame:
#     # Convert orders type_id to integer
#     orders['type_id'] = pd.to_numeric(orders['type_id'])
#     # process orders and evaluate marget status
#     doctrine_orders = orders[orders['type_id'].isin(doctrine_df['type_id'])]
#     most_recent = doctrine_orders['timestamp'].max()
#     doctrine_orders = doctrine_orders[doctrine_orders['timestamp'] == most_recent]
#     doctrine_orders.reset_index(drop=True, inplace=True)
#     aggregate_df = doctrine_orders.groupby('type_id').sum()
#     aggregate_df.reset_index(inplace=True)
#     print(aggregate_df.head())
#     print(aggregate_df.columns)
#     print(doctrine_df.head())
#     print(doctrine_df.columns)
#     final_orders = aggregate_df.drop(columns=['order_id', 'issued', 'duration', 'is_buy_order', 'timestamp'])
#     df = doctrine_df.merge(final_orders, on='type_id', how='left')
#
#     return df


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
        df = pd.read_sql_table('fittings_doctrine_fittings', conn)
        df2 = pd.read_sql_table('fittings_doctrine', conn)
    df.rename(columns={'fitting_id': 'fit_id'}, inplace=True)
    df2.rename(columns={'id': 'doctrine_id'}, inplace=True)
    print(df2.columns)
    df3 = target_df.merge(df, on='fit_id', how='left')
    df4 = df3.merge(df2, on='doctrine_id', how='left')
    return df4


def read_doctrine_watchlist(db_name: str = 'wc_fitting') -> list:
    try:
        # Create the connection string without quotes around database name
        mysql_connection = f"mysql+mysqlconnector://Orthel:Dawson007!27608@localhost:3306/{db_name}"
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
            # Convert to list and return
            id_list = df['type_id'].tolist()
            return id_list

    except exc.OperationalError as e:
        print(f"Database connection error: {str(e)}")
        return []
    except exc.ProgrammingError as e:
        print(f"SQL query error: {str(e)}")
        return []
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return []
    finally:
        if 'engine' in locals():
            engine.dispose()


def clean_doctrine_columns(df: pd.DataFrame) -> pd.DataFrame:
    print(df.columns)
    # df = df.drop(columns=["doctrine_id", "ship_type_id"])
    doctrines = get_doctrine_fits()
    doctrines = doctrines.rename(columns={'name': 'doctrine_name', 'id': 'fit_id'})
    doctrines.drop('type_name', inplace=True, axis=1)

    merged_df = df.merge(doctrines, on='doctrine_name', how='left')
    print(merged_df.head())

    new_cols = ['type_id', 'type_name', 'quantity',
                'volume_remain', 'price', 'fits_on_market', 'delta', 'fit_id', 'doctrine_name', 'doctrine_id',
                'ship_type_id']
    updated_merged_df = merged_df[new_cols]
    return updated_merged_df

if __name__ == "__main__":
    pass
