
import pandas as pd
import sqlalchemy
from sqlalchemy import exc

from db_handler import read_market_orders


def get_doctrine_fits(db_name: str = 'wc_fitting'):
    print('accessing_db')
    print('------------------------')
    mysql_connection = f"mysql+mysqlconnector://Orthel:Dawson007!27608@localhost:3306/{db_name}"
    engine = sqlalchemy.create_engine(mysql_connection)
    print('MySql db connection established')
    print('accessing doctrines...')
    table_name = 'watch_doctrines'
    query = f"SELECT id, name FROM {table_name}"
    df = pd.read_sql_query(query, engine)
    for row in range(len(df)):
        print(df.loc[row, 'name'])

    doctrine_ids = ", ".join(map(str, df['id'].tolist()))

    query = f"SELECT doctrine_id, fitting_id FROM fittings_doctrine_fittings WHERE doctrine_id in ([doctrines_ids]);"

    # Use the ids in the second query
    query = f"""
        SELECT doctrine_id, fitting_id 
        FROM fittings_doctrine_fittings 
        WHERE doctrine_id IN ({doctrine_ids})
    """
    fittings = pd.read_sql_query(query, engine)

    # Get the doctrine names
    doctrine_query = "SELECT id as doctrine_id, name as doctrine_name FROM watch_doctrines"
    doctrine_df = pd.read_sql_query(doctrine_query, engine)

    # Merge with your existing dataframe
    result_df = fittings.merge(doctrine_df, on='doctrine_id', how='left')
    fit_ids = ", ".join(map(str, result_df['fitting_id'].tolist()))

    query = f"""
        SELECT f.id, f.name, f.ship_type_id, t.type_name 
        FROM fittings_fitting f
        JOIN fittings_type t ON t.type_id = f.ship_type_id
        WHERE f.id IN ({fit_ids})
    """
    df = pd.read_sql_query(query, engine)
    print(f'{len(df)} fits retrieved')
    print('closing db connection with engine.dispose()')
    engine.dispose()
    return df

def get_fit_items(df, fit_id: int):
    cols = ['id', 'name', 'ship_type_id', 'type_name']
    db_name = 'wc_fitting'
    mysql_connection = f"mysql+mysqlconnector://Orthel:Dawson007!27608@localhost:3306/{db_name}"
    engine = sqlalchemy.create_engine(mysql_connection)
    filtered_df = df[df['id'] == fit_id]
    df = filtered_df.rename({'id': 'fit_id', 'ship_type_id': 'type_id', 'name': 'doctrine_name'}, axis="columns")

    query = f"""
        SELECT id, type_id, quantity 
        FROM fittings_fittingitem
        WHERE fit_id = {fit_id};
    """

    df2 = pd.read_sql_query(query, engine)
    df2 = df2.groupby('type_id').sum()
    df2.drop(columns=['id'], inplace=True)
    df2.reset_index(inplace=True)

    doctrine_name = df['doctrine_name'].tolist()[0]
    df2['fit_id'] = fit_id
    df2['doctrine_name'] = doctrine_name

    fit_type_ids = ", ".join(map(str, df2['type_id'].tolist()))

    query = f"""
        SELECT type_id as type_id, type_name as type_name from fittings_type
        WHERE type_id IN ({fit_type_ids});
    """
    fit_names = pd.read_sql_query(query, engine)
    engine.dispose()

    df4 = df2.merge(fit_names, on='type_id', how='left')
    df['quantity'] = 1
    fit_items = pd.concat([df, df4])
    return fit_items

def get_market_stats(doctrine_df: pd.DataFrame, orders: pd.DataFrame, target: int = 20) -> pd.DataFrame:
    # Convert orders type_id to integer
    orders['type_id'] = pd.to_numeric(orders['type_id'])
    sell_orders = orders[orders['is_buy_order'] == False]
    # process orders and evaluate marget status
    doctrine_orders = sell_orders[sell_orders['type_id'].isin(doctrine_df['type_id'])]
    most_recent = doctrine_orders['timestamp'].max()
    doctrine_orders = doctrine_orders[doctrine_orders['timestamp'] == most_recent]
    doctrine_orders.reset_index(drop=True, inplace=True)
    aggregate_df = doctrine_orders.groupby('type_id').sum()
    aggregate_df.reset_index(inplace=True)
    final_orders = aggregate_df.drop(columns=['order_id', 'issued', 'duration', 'is_buy_order', 'timestamp'])
    df = doctrine_df.merge(final_orders, on='type_id', how='left')
    return df

def get_doctrine_status(target: int = 20) -> pd.DataFrame:
    target_df = pd.DataFrame()
    fits_df = get_doctrine_fits(db_name='wc_fitting')
    fit_ids = fits_df['id'].tolist()
    c = 1
    print('accessing SQLLite DB....\n'
          'reading market_orders...')
    print('------------------------')
    orders = read_market_orders()
    print(f'{len(orders)} retrieved')
    print('processing market stats...')

    for id in fit_ids:
        fit_items = get_fit_items(fits_df, id)
        c += 1
        markets = get_market_stats(doctrine_df=fit_items, orders=orders, target=target)
        if markets is not None:
            target_df = pd.concat([target_df, markets])
        else:
            continue
    target_df['fits_on_market'] = target_df['volume_remain'] / target_df['quantity']
    target_df['fits_on_market'] = target_df['fits_on_market'].round(0)
    target_df['delta'] = target_df['fits_on_market'] - target
    target_df.drop(columns=['type_name_y'], inplace=True)
    target_df.rename(columns={'type_name_x': 'type_name'}, inplace=True)

    target_ids = target_df['fit_id'].unique().tolist()
    short_df = pd.DataFrame(columns=target_df.columns)

    c = 0
    dfs = []
    for id in target_ids:
        df = target_df[target_df['fit_id'] == id]
        df2 = df[df['fits_on_market'] < target]
        dfs.append(df2)
        print(f'\rparsing fit {c} of {len(target_ids)}', end='')
        c += 1
    short_df = pd.concat(dfs, ignore_index=True)

    summary_df = target_df.groupby('fit_id').agg({
        'fits_on_market': 'min',
        'doctrine_name': 'first'
    }).reset_index()
    return short_df, target_df, summary_df

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
    doctrines = get_doctrine_fits()
    doctrines = doctrines.rename(columns={'name': 'doctrine_name', 'id': 'doctrine_id'})
    doctrines.drop('type_name', inplace=True, axis=1)

    df = df.merge(doctrines, on='doctrine_name', how='left')

    new_cols = ['type_id', 'type_name', 'quantity',
                'volume_remain', 'price', 'fits_on_market', 'delta', 'fit_id', 'doctrine_name', 'doctrine_id',
                'ship_type_id']
    df = df[new_cols]
    return df

if __name__ == "__main__":
    pass
