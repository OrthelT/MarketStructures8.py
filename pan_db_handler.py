import sqlite3

import pandas as pd
import os
import sqlite3 as sq


def initiate_connection(database: str = 'market_orders.sqlite'):
    conn = sqlite3.connect(database)
    return conn


def write_db(data):
    conn = initiate_connection()
    df = pd.DataFrame(data)
    df.to_sql('orders', conn, if_exists='append', index=False)
    conn.close()


def read_market_orders():
    conn = sqlite3.connect('market_orders.sqlite')
    orders = pd.read_sql('SELECT * FROM current_orders', conn)
    conn.close()
    return orders


def dict_factory(cursor, row):
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}


def read_watchlist():
    conn = initiate_connection(database=r'../ESI_Utilities/SDE/SDE sqlite-latest.sqlite')
    df = pd.read_sql('SELECT * FROM watchlist', conn)
    conn.close()
    return df


if __name__ == '__main__':
    print('lll')
