import sqlite3
from contextlib import closing

import pandas as pd
import time

from pandas import notnull, isnull, notna
from pandas.core.computation.ops import isnumeric


def initiate_connection(database: str = "market_orders.sqlite"):
    conn = sqlite3.connect(database)
    return conn


def write_db(data):
    conn = initiate_connection()
    df = pd.DataFrame(data)
    df.to_sql("orders", conn, if_exists="append", index=False)
    conn.close()


def read_market_orders():
    conn = sqlite3.connect("market_orders.sqlite")
    print(f'connection established: {conn} by pan_db_handler')
    orders = pd.read_sql("SELECT * FROM current_orders", conn)
    conn.close()
    print(f'connection closed: {conn}...returning orders from pan_db_handler')
    return orders


def dict_factory(cursor, row):
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}


def read_watchlist():
    conn = initiate_connection(
        database=r"../ESI_Utilities/SDE/SDE sqlite-latest.sqlite"
    )
    df = pd.read_sql("SELECT * FROM watchlist", conn)
    conn.close()
    return df


# def read_doctrine():
#     conn = initiate_connection(
#         database=r"../ESI_Utilities/SDE/SDE sqlite-latest.sqlite"
#     )
#     df = pd.read_sql("SELECT * FROM doctrines", conn)


if __name__ == "__main__":
    pass
