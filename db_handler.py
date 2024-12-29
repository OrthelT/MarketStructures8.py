import sqlite3
import pandas as pd
import sqlalchemy as sq

mkt_sqlfile = "sqlite:///market_orders.sqlite"
fit_mysqlfile = "mysql+pymysql://Orthel:Dawson007!27608@localhost:3306/wc_fitting"


def initiate_connection(database: str = "market_orders.sqlite"):
    conn = sqlite3.connect(database)
    return conn


def read_market_orders():
    conn = sqlite3.connect("market_orders.sqlite")
    print(f'connection established: {conn} by db_handler')
    orders = pd.read_sql("SELECT * FROM current_orders", conn)
    conn.close()
    print(f'connection closed: {conn}...returning orders from pan_db_handler')
    return orders


def read_market_stats() -> pd.DataFrame:
    engine = sq.create_engine("sqlite:///market_orders.sqlite")
    conn = engine.connect()
    print(f'connection established: {conn} by db_handler')
    stats = pd.read_sql("SELECT * FROM Market_Stats", conn)
    conn.close()
    print(f'connection closed: {conn}...returning orders from pan_db_handler')
    return stats


def read_watchlist() -> pd.DataFrame:
    conn = initiate_connection(
        database=r"market_orders.sqlite"
    )
    df = pd.read_sql("SELECT * FROM watchlist_mkt", conn)
    conn.close()
    df = df.rename(
        columns={
            "typeID": "type_id",
        })
    return df


def read_history(doys: int = 30):
    conn = sqlite3.connect("market_orders.sqlite")
    print(f'connection established: {conn} by pan_db_handler')
    d = f"'-{doys} days'"
    history = pd.read_sql(f"""
    SELECT * FROM market_history
    WHERE date >= date('now', {d})""", conn)
    conn.close()
    print(f'connection closed: {conn}...returning orders from db_handler')
    return history


def read_doctrine_items():
    stmt = """
    SELECT ffi.type_id, ffi.quantity
    FROM fittings_fittingitem ffi
    WHERE fit_id IN (
        select fdf.fitting_id
            from fittings_doctrine_fittings fdf
            where doctrine_id in (
                select id
                from watch_doctrines)
            );
    """
    engine = sq.create_engine("mysql://Orthel:Dawson007!27608@localhost:3306/wc_fitting")
    conn = engine.connect()
    print(f'connection established: {conn} by db_handler')
    df = pd.read_sql(stmt, conn)
    conn.close()
    print(f'connection closed: {conn}...returning orders from pan_db_handler')
    return df


if __name__ == "__main__":
    pass
