from datetime import datetime, timezone
from typing import Optional, List

import pandas as pd
import polars as pl
from sqlalchemy import (
    create_engine,
    String,
    Integer,
    Float,
    DateTime,
    Boolean,
    PrimaryKeyConstraint,
    text,
    Table, MetaData, Column, )
from sqlalchemy.orm import declarative_base, mapped_column, sessionmaker, Mapped

from logging_tool import configure_logging

sql_logger = configure_logging(
    "sql_logger",
    "logs/sql_logger.log")

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


class MarketOrder(Base):
    __tablename__ = "market_order"

    order_id: Mapped[int] = mapped_column(primary_key=True)
    type_id: Mapped[str] = mapped_column(String(10))
    type_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    volume_remain: Mapped[int] = mapped_column(Integer)
    price: Mapped[float] = mapped_column(Float)
    issued: Mapped[datetime] = mapped_column(DateTime)
    duration: Mapped[int] = mapped_column(Integer)
    is_buy_order: Mapped[bool] = mapped_column(Boolean)
    timestamp: Mapped[datetime] = mapped_column(DateTime)


class MarketHistory(Base):
    __tablename__ = "market_history"
    date: Mapped[datetime] = mapped_column(DateTime)
    type_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    type_id: Mapped[str] = mapped_column(String(10))
    average: Mapped[float] = mapped_column(Float)
    volume: Mapped[int] = mapped_column(Integer)
    highest: Mapped[float] = mapped_column(Float)
    lowest: Mapped[float] = mapped_column(Float)
    order_count: Mapped[int] = mapped_column(Integer)
    timestamp: Mapped[datetime] = mapped_column(DateTime)

    # Add composite primary key
    __table_args__ = (PrimaryKeyConstraint("date", "type_id"),)


class MarketStats(Base):
    __tablename__ = "Market_Stats"
    type_id: Mapped[str] = mapped_column(String(10), primary_key=True)
    total_volume_remain: Mapped[int] = mapped_column(Integer, nullable=True)
    min_price: Mapped[float] = mapped_column(Float, nullable=True)
    price_5th_percentile: Mapped[float] = mapped_column(Float, nullable=True)
    avg_of_avg_price: Mapped[float] = mapped_column(Float, nullable=True)
    avg_daily_volume: Mapped[float] = mapped_column(Float, nullable=True)
    group_id: Mapped[str] = mapped_column(String(10))
    type_name: Mapped[Optional[str]] = mapped_column(String(100))
    group_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    category_id: Mapped[str] = mapped_column(String(10))
    category_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    days_remaining: Mapped[int] = mapped_column(Integer, nullable=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime)


class Doctrine_Fits(Base):
    __tablename__ = "Doctrine_Fittings"
    doctrine_id: Mapped[int] = mapped_column(Integer)
    doctrine_name: Mapped[str] = mapped_column(String(100))
    doctrine_version: Mapped[str] = mapped_column(String(100))
    fitting_name: Mapped[str] = mapped_column(String(100), primary_key=True)
    ship_class: Mapped[str] = mapped_column(String(50))
    ship_group: Mapped[str] = mapped_column(String(50))
    ship_group_id: Mapped[str] = mapped_column(String(100))


class Fitting_Items(Base):
    __tablename__ = "Fittings"
    type_id: Mapped[str] = mapped_column(String(10), primary_key=True)
    fitting_name: Mapped[str] = mapped_column(String(100))
    type_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    qty_required: Mapped[int] = mapped_column(Integer)
    fitting_version: Mapped[int] = mapped_column(Integer)
    is_hull: Mapped[bool] = mapped_column(Boolean)


class CurrentOrders(Base):
    __tablename__ = "current_orders"
    order_id: Mapped[int] = mapped_column(primary_key=True)
    type_id: Mapped[str] = mapped_column(String(10))
    type_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    volume_remain: Mapped[int] = mapped_column(Integer)
    price: Mapped[float] = mapped_column(Float)
    issued: Mapped[datetime] = mapped_column(DateTime)
    duration: Mapped[int] = mapped_column(Integer)
    is_buy_order: Mapped[bool] = mapped_column(Boolean)
    timestamp: Mapped[datetime] = mapped_column(DateTime)


class ShortItems(Base):
    __tablename__ = "ShortItems"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    fit_id: Mapped[int] = mapped_column(Integer)
    doctrine_name: Mapped[str] = mapped_column(String(100), nullable=True)
    type_id: Mapped[int] = mapped_column(Integer)
    type_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    quantity: Mapped[int] = mapped_column(Integer, nullable=True)
    volume_remain: Mapped[float] = mapped_column(Float, nullable=True)
    price: Mapped[float] = mapped_column(Float, nullable=True)
    fits_on_market: Mapped[int] = mapped_column(Integer, nullable=True)
    delta: Mapped[float] = mapped_column(Float, nullable=True)
    doctrine_id: Mapped[int] = mapped_column(Integer, nullable=True)
    ship_type_id: Mapped[int] = mapped_column(Integer, nullable=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=True)


class Doctrine_Items(Base):
    __tablename__ = "Doctrine_Items"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    fit_id: Mapped[int] = mapped_column(Integer)
    doctrine_name: Mapped[str] = mapped_column(String(100), nullable=True)
    type_id: Mapped[int] = mapped_column(Integer)
    type_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    quantity: Mapped[int] = mapped_column(Integer, nullable=True)
    volume_remain: Mapped[float] = mapped_column(Float, nullable=True)
    price: Mapped[float] = mapped_column(Float, nullable=True)
    fits_on_market: Mapped[int] = mapped_column(Integer, nullable=True)
    delta: Mapped[float] = mapped_column(Float, nullable=True)
    doctrine_id: Mapped[int] = mapped_column(Integer, nullable=True)
    ship_type_id: Mapped[int] = mapped_column(Integer, nullable=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=True)


class MarketBasket(Base):
    __tablename__ = "MarketBasket"

    type_id: Mapped[int] = mapped_column(Integer)
    date: Mapped[datetime] = mapped_column(DateTime)
    buy_price_low: Mapped[float] = mapped_column(Float)
    buy_price_avg: Mapped[float] = mapped_column(Float)
    buy_price_high: Mapped[float] = mapped_column(Float)
    sell_price_low: Mapped[float] = mapped_column(Float)
    sell_price_avg: Mapped[float] = mapped_column(Float)
    sell_price_high: Mapped[float] = mapped_column(Float)
    buy_volume_low: Mapped[int] = mapped_column(Integer)
    buy_volume_avg: Mapped[int] = mapped_column(Integer)
    buy_volume_high: Mapped[int] = mapped_column(Integer)
    sell_volume_low: Mapped[int] = mapped_column(Integer)
    sell_volume_avg: Mapped[int] = mapped_column(Integer)
    sell_volume_high: Mapped[int] = mapped_column(Integer)
    ore: Mapped[str] = mapped_column(String(100))
    qty: Mapped[int] = mapped_column(Integer)

    __table_args__ = (PrimaryKeyConstraint("date", "type_id"),)


def process_dataframe(
        df: pl.DataFrame, columns: list, date_column: str = None
) -> pl.DataFrame:
    """Process the dataframe by selecting columns and converting dates."""
    # Select only the specified columns
    df = df.select(columns)
    if date_column is None:
        if "date" in df.columns:
            date_column = "date"
        elif "issued" in df.columns:
            date_column = "issued"
        else:
            date_column = None
    # Convert  date strings to datetime objects
    if date_column:
        df = df.with_columns(
            [pl.col(date_column).str.strptime(pl.Datetime).alias(date_column)]
        )
    df = insert_timestamp(df)
    return df


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


def insert_type_names(df: pl.DataFrame) -> pl.DataFrame:
    engine = create_engine(mkt_sqlfile, echo=False)

    match_type_ids = """
       SELECT typeID, typeName FROM JoinedInvTypes
       """
    with engine.connect() as conn:
        result = conn.execute(text(match_type_ids))
        type_mappings = result.fetchall()

    type_dict = dict(type_mappings)

    df_named = df.with_columns(
        pl.col("type_id")
        .map_elements(lambda x: type_dict.get(x), return_dtype=pl.String)
        .alias("type_name")
    )
    return df_named


def insert_pd_type_names(df: pd.DataFrame) -> pd.DataFrame:
    engine = create_engine(mkt_sqlfile, echo=False)

    match_type_ids = """
       SELECT typeID, typeName FROM JoinedInvTypes
       """

    with engine.connect() as conn:
        result = conn.execute(text(match_type_ids))
        type_mappings = result.fetchall()
        sql_logger.info(print(type_mappings[:10]))
        sql_logger.info(print(f'type: {type(type_mappings)}'))

    names = pd.DataFrame(type_mappings, columns=['type_id', 'type_name'])
    sql_logger.info(print(names))

    df2 = df.copy()
    df2 = df2.merge(names, on='type_id', how='left')

    return df2


def insert_timestamp(df: pl.DataFrame) -> pl.DataFrame:
    ts = datetime.now(timezone.utc)
    df = df.with_columns(
        pl.lit(ts).alias("timestamp"),
    )
    return df


def insert_pd_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    ts = datetime.now(timezone.utc)

    # detect if timestamp already exists
    if 'timestamp' in df2.columns:
        if pd.api.types.is_datetime64_any_dtype(df2['timestamp']):
            print('timestamp exists')
            return df2
        else:
            df2.drop(columns=['timestamp'], inplace=True)

    df2.loc[:, "timestamp"] = ts
    return df2


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
            print(f"Error updating orders: {str(e)}")
            status = f"Error updating orders: {str(e)}"
            raise

    return f"{status} Doctrine items loading completed successfully!"


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


def read_history(doys: int = 30) -> pd.DataFrame:
    engine = create_engine(mkt_sqlfile, echo=False)

    # Create a session factoryf
    session = engine.connect()
    print(f'connection established: {session} by sql_handler.read_history()')
    d = f"'-{doys} days'"

    stmt = f"""
    SELECT * FROM market_history
    WHERE date >= date('now', {d})"""

    historydf = pd.read_sql(stmt, session)
    session.close()
    print(f'connection closed: {session}...returning orders from market_history table.')
    return historydf


def update_current_orders(df: pl.DataFrame) -> str:
    df_processed = insert_type_names(df)
    records = df_processed.to_dicts()

    engine = create_engine(f"sqlite:///{sql_file}", echo=False)
    with engine.connect() as conn:
        batch_size = 1000
        status = "failed"
        current_statement = """
            INSERT INTO current_orders
                (order_id, type_id, type_name, volume_remain, price, issued, duration, is_buy_order, timestamp)
                VALUES
                (:order_id, :type_id, :type_name, :volume_remain, :price, :issued, :duration, :is_buy_order, :timestamp);
            """

        clear_table = "DELETE FROM current_orders;"

        try:
            conn.execute(text(clear_table))
            conn.commit()
        except Exception as e:
            print(f"Error clearing table: {str(e)}")
            raise
        print("table cleared")

        try:
            for i in range(0, len(records), batch_size):
                batch = records[i: i + batch_size]
                conn.execute(text(current_statement), batch)
                conn.commit()
                print(
                    f"\rProcessed records {i} to {min(i + batch_size, len(records))}",
                    end="",
                )
        except Exception as e:
            print(f"Error inserting data: {str(e)}")
            raise

    status = "Data loading completed successfully!"

    return status


def update_stats(df: pd.DataFrame) -> str:
    df = df.infer_objects()
    df = df.fillna(0)

    df_processed = insert_pd_timestamp(df)

    records = df_processed.to_dict()

    # start a session
    engine = create_engine(f"sqlite:///{sql_file}", echo=False)
    Session = sessionmaker(bind=engine)

    with Session() as session:  # Corrected the session instantiation
        try:
            # Clear the table
            session.query(MarketStats).delete()
            session.commit()
            print("Table cleared")

            # Insert new records
            batch_size = 1000
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                stats_objects = [
                    MarketStats(
                        type_id=record["type_id"],
                        total_volume_remain=record["total_volume_remain"],
                        min_price=record["min_price"],
                        price_5th_percentile=record["price_5th_percentile"],
                        avg_of_avg_price=record["avg_of_avg_price"],
                        avg_daily_volume=record["avg_daily_volume"],
                        group_id=record["group_id"],
                        type_name=record["type_name"],
                        group_name=record["group_name"],
                        category_id=record["category_id"],
                        category_name=record["category_name"],
                        days_remaining=record["days_remaining"],
                        timestamp=record["timestamp"],
                    )
                    for record in batch
                ]
                session.add_all(stats_objects)
                session.commit()
                print(
                    f"\rProcessed records {i} to {min(i + batch_size, len(records))}",
                    end="",
                )

        except Exception as e:
            session.rollback()
            print(f"Error occurred: {str(e)}")
            raise
        finally:
            session.close()
            return "Stats loading completed successfully!"


def update_stats2(df: pd.DataFrame) -> str:
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
            print("Table cleared")

        df_processed.to_sql('Market_Stats', engine,
                            if_exists='replace',
                            index=False,
                            method='multi',
                            chunksize=1000)
        status = "Data loading completed successfully!"
        missing_status = fill_missing_stats()
        status = status + missing_status
        return status
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise


def fill_missing_stats() -> str:
    stats = read_sql_market_stats()
    watchlist = read_sql_watchlist()

    stats['type_id'] = stats['type_id'].astype(int)

    missing = watchlist[~watchlist['type_id'].isin(stats['type_id'])]

    missing_df = pd.DataFrame(
        columns=['type_id', 'total_volume_remain', 'min_price', 'price_5th_percentile',
                 'avg_of_avg_price', 'avg_daily_volume', 'group_id', 'type_name',
                 'group_name', 'category_id', 'category_name', 'days_remaining', 'timestamp'])
    missing_df = pd.concat([missing, missing_df])
    missing_df['timestamp'] = stats['timestamp']
    missing_df['total_volume_remain'] = stats['total_volume_remain']

    # fill historical values where available
    hist = read_history(30)
    hist_grouped = hist.groupby("type_id").agg({'average': 'mean', 'volume': 'mean'})
    missing_df['avg_of_avg_price'] = missing_df['type_id'].map(hist_grouped['average'])
    missing_df['avg_daily_volume'] = missing_df['type_id'].map(hist_grouped['volume'])

    # deal with remaining null values
    missing_df = missing_df.infer_objects()
    missing_df.fillna(0, inplace=True)

    # update the database
    engine = create_engine(mkt_sqlfile, echo=False)
    try:
        with engine.connect() as conn:
            missing_df.to_sql('Market_Stats', engine,
                              if_exists='append',
                              index=False,
                              method='multi',
                              chunksize=1000
                              )
        return "missing Stats loading completed successfully!"
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise

def update_short_items(df: pd.DataFrame) -> str:
    # process the df
    df = df.fillna("").replace([float('inf'), float('-inf')], 0)
    df_pl = pl.from_pandas(df)
    df_processed = insert_timestamp(df_pl)
    df_pl.fill_null(0)
    records = df_processed.to_dicts()
    # start a session
    engine = create_engine(f"sqlite:///{sql_file}", echo=False)
    Session = sessionmaker(bind=engine)

    with Session() as session:  # Corrected the session instantiation
        try:

            # Clear the table
            session.query(ShortItems).delete()
            session.commit()
            print("Table cleared")

            # Insert new records
            batch_size = 1000
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                short_objects = [
                    ShortItems(
                        fit_id=record["fit_id"],
                        doctrine_name=record["doctrine_name"],
                        type_id=record["type_id"],
                        type_name=record["type_name"],
                        quantity=record["quantity"],
                        volume_remain=record["volume_remain"],
                        price=record["price"],
                        fits_on_market=record["fits_on_market"],
                        delta=record["delta"],
                        doctrine_id=record["doctrine_id"],
                        ship_type_id=record["ship_type_id"],
                        timestamp=record["timestamp"]

                    )

                    for record in batch
                ]
                session.add_all(short_objects)
                session.commit()

                print(
                    f"\rProcessed records {i} to {min(i + batch_size, len(records))}",
                    end="",
                )
        except Exception as e:
            session.rollback()
            print(f"Error occurred: {str(e)}")
            raise

    return "Short items loading completed successfully!"


def update_short_items_optimized(df: pd.DataFrame) -> str:
    # process the df
    df_processed = insert_pd_timestamp(df)
    status = "processed data"

    # start a session
    engine = create_engine(f"sqlite:///{sql_file}", echo=False)

    with engine.connect():
        try:
            df_processed.to_sql('market_order', con=engine, if_exists='replace', index=False, chunksize=1000)
            status += ", data loaded"
        except Exception as e:
            sql_logger.error(print(f'an exception occurred in df_processed.to_sql: {e}'))
            raise

    return f"{status} Short items loading completed successfully!"


def read_short_items() -> pd.DataFrame:
    engine = create_engine(f"sqlite:///{sql_file}", echo=False)
    df = pd.read_sql_query("SELECT * FROM ShortItems", engine)
    engine.dispose()
    print(f'connection closed: {engine}...returning orders from ShortItems table.')

    return df


def read_doctrine_items() -> pd.DataFrame:
    engine = create_engine(f"sqlite:///{sql_file}", echo=False)
    df = pd.read_sql_query("SELECT * FROM Doctrine_Items", engine)
    engine.dispose()
    print(f'connection closed: {engine}...returning orders from ShortItems table.')

    return df


def create_joined_invtypes_table():
    sql_logger.info("Creating joined_invtypes table...")
    # Define SQLite and MySQL database URIs
    sqlite_uri = f"sqlite:///{mkt_sqlfile}"
    mysql_uri = f"mysql+pymysql://{fit_sqlfile}"

    # Create engines for both databases
    sqlite_engine = create_engine(sqlite_uri, echo=False)
    mysql_engine = create_engine(mysql_uri, echo=False)

    # Reflect the SQLite database schema
    metadata = MetaData()
    sqlite_table = Table(
        "JoinedInvTypes",
        metadata,
        Column("typeID", Integer),
        Column("groupID", Integer),
        Column("typeName", String(255)),  # Updated to include length
        Column("groupName", String(255)),  # Updated to include length
        Column("categoryID", Integer),
        Column("categoryID_2", Integer),
        Column("categoryName", String(255)),  # Updated to include length
        Column("metaGroupID", Integer),
        Column("metaGroupID_2", Integer),
        Column("metaGroupName", String(255)),  # Updated to include length
    )

    # Define the same table in MySQL
    mysql_metadata = MetaData()
    mysql_table = Table(
        "JoinedInvTypes",
        mysql_metadata,
        Column("typeID", Integer),
        Column("groupID", Integer),
        Column("typeName", String(255)),  # Updated to include length
        Column("groupName", String(255)),  # Updated to include length
        Column("categoryID", Integer),
        Column("categoryID_2", Integer),
        Column("categoryName", String(255)),  # Updated to include length
        Column("metaGroupID", Integer),
        Column("metaGroupID_2", Integer),
        Column("metaGroupName", String(255)),  # Updated to include length
    )

    # Create the table in MySQL
    mysql_metadata.create_all(mysql_engine)

    # Transfer data from SQLite to MySQL
    SessionSQLite = sessionmaker(bind=sqlite_engine)
    SessionMySQL = sessionmaker(bind=mysql_engine)

    sqlite_session = SessionSQLite()
    mysql_session = SessionMySQL()

    try:
        # Fetch all data from the SQLite table
        # Reflect and map the table explicitly
        sqlite_table = metadata.tables.get("JoinedInvTypes")

        # Fetch all rows from the table
        data = sqlite_session.execute(sqlite_table.select()).fetchall()

        # Insert data into the MySQL table
        for row in data:
            insert_data = {
                "typeID": row.typeID,
                "groupID": row.groupID,
                "typeName": row.typeName,
                "groupName": row.groupName,
                "categoryID": row.categoryID,
                "categoryID_2": row.categoryID_2,
                "categoryName": row.categoryName,
                "metaGroupID": row.metaGroupID,
                "metaGroupID_2": row.metaGroupID_2,
                "metaGroupName": row.metaGroupName,
            }
            mysql_session.execute(mysql_table.insert().values(insert_data))

        # Commit changes to MySQL
        mysql_session.commit()
        print("Data transfer completed successfully!")
    except Exception as e:
        mysql_session.rollback()
        print(f"An error occurred: {e}")
    finally:
        sqlite_session.close()
        mysql_session.close()


def get_missing_icons():
    SDEsql = '../ESI_Utilities/SDE/SDE sqlite-latest.sqlite'
    mysql_uri = f"mysql+pymysql://{fit_sqlfile}"
    SDE_uri = f"sqlite:///{SDEsql}"

    engine = create_engine(SDE_uri, echo=False)
    con = engine.connect()
    df = pd.read_sql_query("""
       SELECT j.typeID, j.iconID, i.categoryID FROM Joined_InvTypes j
       join invGroups i on j.groupID = i.groupId
       WHERE i.categoryID = 6

       """, con)

    con.close()
    print(len(df))


def update_doctrine_items(df: pd.DataFrame) -> str:
    # process the df
    df_pl = pl.from_pandas(df)
    df_processed = insert_timestamp(df_pl)
    df_pl.fill_null(0)
    records = df_processed.to_dicts()
    # start a session
    engine = create_engine(f"sqlite:///market_orders.sqlite", echo=False)
    Session = sessionmaker(bind=engine)

    with Session() as session:  # Corrected the session instantiation
        try:

            # Clear the table
            session.query(Doctrine_Items).delete()
            session.commit()
            print("Table cleared")

            # Insert new records
            batch_size = 1000
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                doctrine_objects = [
                    Doctrine_Items(
                        fit_id=record["fit_id"],
                        doctrine_name=record["doctrine_name"],
                        type_id=record["type_id"],
                        type_name=record["type_name"],
                        quantity=record["quantity"],
                        volume_remain=record["volume_remain"],
                        price=record["price"],
                        fits_on_market=record["fits_on_market"],
                        delta=record["delta"],
                        doctrine_id=record["doctrine_id"],
                        ship_type_id=record["ship_type_id"],
                        timestamp=record["timestamp"]
                    )

                    for record in batch
                ]
                session.add_all(doctrine_objects)
                session.commit()

                print(
                    f"\rProcessed records {i} to {min(i + batch_size, len(records))}",
                    end="",
                )
        except Exception as e:
            session.rollback()
            print(f"Error occurred: {str(e)}")
            raise

    return "Doctrine items loading completed successfully!"


def optimize_for_bulk_update(engine):
    # Optimize database settings for bulk insert
    with engine.begin() as conn:
        conn.execute(text("PRAGMA synchronous = OFF;"))
        conn.execute(text("PRAGMA journal_mode = MEMORY;"))


def revert_sqlite_settings(engine):
    # Revert SQLite to safer defaults
    with engine.begin() as conn:
        conn.execute(text("PRAGMA synchronous = FULL;"))
        conn.execute(text("PRAGMA journal_mode = DELETE;"))


def read_sql_watchlist() -> pd.DataFrame:
    engine = create_engine(mkt_sqlfile, echo=False)
    with engine.connect() as conn:
        df = pd.read_sql_table('watchlist_mkt', conn)
    df = df.rename(
        columns={
            "typeID": "type_id",
        })
    return df


def read_sql_market_stats() -> pd.DataFrame:
    engine = create_engine(mkt_sqlfile, echo=False)
    with engine.connect() as conn:
        df = pd.read_sql_table('Market_Stats', conn)
    return df


def validate_dataframe(df: pd.DataFrame):
    validated_data = []
    errors = []

    for index, row in df.iterrows():
        try:
            record = MarketStats(**row.to_dict())
            validated_data.append(record)
        except Exception as e:
            errors.append((index, str(e)))

    return validated_data, errors


def update_market_basket(df: pd.DataFrame) -> str:
    sql_logger.info("Updating market basket...")
    engine = create_engine(mkt_sqlfile, echo=True)
    with engine.connect() as conn:
        df.to_sql('MarketBasket', con=conn, if_exists='append', index=False, chunksize=1000)
    engine.dispose()
    return "Market basket loading completed successfully!"

if __name__ == "__main__":
    pass