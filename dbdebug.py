import pandas as pd
import sqlalchemy
from logging_tool import configure_logging


def read_doctrine_watchlist(db_name: str = 'wc_fitting') -> list:
    logger.info(f'connecting to {db_name}')
    mysql_connection = f"mysql+mysqlconnector://Orthel:Dawson007!27608@localhost:3306/{db_name}"
    engine = sqlalchemy.create_engine(mysql_connection)
    conn = engine.connect()
    logger.info(f'MySql db connection established')

    query = """
    SELECT DISTINCT t.type_id,ft.type_name
    FROM watch_doctrines as w
    JOIN fittings_doctrine_fittings f on w.id = f.doctrine_id
    JOIN fittings_fittingitem t on f.fitting_id = t.fit_id
    JOIN fittings_type ft on t.type_id = ft.type_id
    """

    logger.info(f'executing {query} on {engine}')
    df = pd.read_sql_query(query, conn)

    print(f'{len(df)} doctrines retrieved')
    print('closing db connection with engine.dispose()')
    engine.dispose()
    id_list = df['type_id'].tolist()
    print(f'{len(id_list)} doctrines retrieved')
    print(id_list[:10])
    return id_list
