
import polars as pl
import pandas as pd
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    create_engine,
    String,
    Integer,
    Float,
    DateTime,
    Boolean,
    PrimaryKeyConstraint,
    text,
    Table, MetaData, Column,


)
from sqlalchemy.orm import DeclarativeBase, declarative_base, mapped_column, sessionmaker, foreign, Mapped
import pymysql
import sqlite3
import logging

logger = logging.getLogger(__name__)
logging.basicConfig()
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

sql_file = "market_orders.sqlite"
mkt_sqlfile = "market_orders.sqlite"
fit_sqlfile = "Orthel:Dawson007!27608@localhost:3306/wc_fitting"

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
    total_volume_remain: Mapped[int] = mapped_column(Integer)
    min_price: Mapped[float] = mapped_column(Float)
    price_5th_percentile: Mapped[float] = mapped_column(Float)
    avg_of_avg_price: Mapped[float] = mapped_column(Float)
    avg_of_avg_price: Mapped[float] = mapped_column(Float, nullable=True)
    avg_daily_volume: Mapped[float] = mapped_column(Float)
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
    doctrine_name: Mapped[str] = mapped_column(String(100))
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

def create_session():
    engine = create_engine(f"sqlite:///{sql_file}", echo=False)
    Session = sessionmaker(bind=engine)

    return Session, engine

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

def insert_type_names(df: pl.DataFrame) -> pl.DataFrame:
    engine = create_engine(f"sqlite:///{sql_file}", echo=False)

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

def insert_timestamp(df: pl.DataFrame) -> pl.DataFrame:
    ts = datetime.now(timezone.utc)
    df = df.with_columns(
        pl.lit(ts).alias("timestamp"),
    )
    print(df)
    return df

def initialize_database(engine, base):
    # # """Create all database tables."""
    # base.metadata.create_all(engine)
    pass

def process_esi_market_order(data: list, is_history: Boolean = False) -> str:
    engine = create_engine(f"sqlite:///{sql_file}", echo=False)
    initialize_database(engine, Base)

    if is_history:
        columns = history_columns
        order_class = "history"
        # update history data
        stmt = """
        INSERT INTO market_history 
            (date, type_id, average, volume, highest, lowest, order_count, timestamp)
        VALUES 
            (:date, :type_id, :average, :volume, :highest, :lowest, :order_count, :timestamp)
        ON CONFLICT(date, type_id) DO UPDATE SET
            average = EXCLUDED.average,
            volume = EXCLUDED.volume,
            highest = EXCLUDED.highest,
            lowest = EXCLUDED.lowest,
            order_count = EXCLUDED.order_count,
            timestamp = EXCLUDED.timestamp
        """
        # Fill in type names
        stmt2 = """
        UPDATE market_history
        SET type_name = (
            select jt.typeName
            from JoinedInvTypes as jt
            where market_history.type_id = jt.typeID
            )
        WHERE type_name is NULL
          AND EXISTS(
          SELECT 1
          FROM JoinedInvTypes as jt
          WHERE jt.typeID = market_history.type_id
          );"""

    else:
        # update market orders
        columns = market_columns
        stmt = """
        INSERT INTO market_order 
            (order_id, type_id, volume_remain, price, issued, duration, is_buy_order, timestamp)
        VALUES 
            (:order_id, :type_id, :volume_remain, :price, :issued, :duration, :is_buy_order, :timestamp)
        ON CONFLICT(order_id) DO UPDATE SET
            volume_remain = EXCLUDED.volume_remain,
            price = EXCLUDED.price,
            issued = EXCLUDED.issued,
            duration = EXCLUDED.duration,
            is_buy_order = EXCLUDED.is_buy_order,
            timestamp = EXCLUDED.timestamp
        """
        # Fill in type_names
        stmt2 = """
        UPDATE market_order
        SET type_name = (select jt.typeName
                         from JoinedInvTypes as jt
                         where market_order.type_id = jt.typeID
        )
        WHERE type_name is NULL
        AND EXISTS(
            SELECT 1
            FROM JoinedInvTypes as jt
            WHERE jt.typeID = market_order.type_id

        );"""

        order_class = "market"

    df = pl.DataFrame(data)
    df_processed = process_dataframe(df, columns)

    records = df_processed.to_dicts()

    batch_size = 1000
    status = "failed"

    with engine.begin() as conn:
        try:
            for i in range(0, len(records), batch_size):
                batch = records[i: i + batch_size]
                conn.execute(text(stmt), batch)
                print(
                    f"\rProcessed records {i} to {min(i + batch_size, len(records))}",
                    end="",
                )
            status = "Data loading completed successfully!"
        except Exception as e:
            print(f"Error inserting data: {str(e)}")
            raise

        try:
            conn.execute(text(stmt2))
        except Exception as e:
            print(f"Error filling in type names: {str(e)}")

    if order_class == "market":
        try:
            status = update_current_orders(df_processed)
        except Exception as e:
            print(f"Error updating current orders: {str(e)}")
            raise

    return status

def read_history(doys: int = 30) -> pd.DataFrame:
    engine = create_engine(f"sqlite:///{sql_file}", echo=True)
    # Create a session factory
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
    # process the df
    df.fillna(0)
    df_pl = pl.from_pandas(df)
    df_processed = insert_timestamp(df_pl)

    records = df_processed.to_dicts()

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

    return "Data loading completed successfully!"

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


def read_short_items() -> pd.DataFrame:
    engine = create_engine(f"sqlite:///{sql_file}", echo=True)
    df = pd.read_sql_query("SELECT * FROM ShortItems", engine)
    engine.dispose()
    print(f'connection closed: {engine}...returning orders from ShortItems table.')

    return df


def create_joined_invtypes_table():
    logger.info("Creating joined_invtypes table...")
    # Define SQLite and MySQL database URIs
    sqlite_uri = f"sqlite:///{mkt_sqlfile}"
    mysql_uri = f"mysql+pymysql://{fit_sqlfile}"

    # Create engines for both databases
    sqlite_engine = create_engine(sqlite_uri, echo=True)
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

    engine = create_engine(SDE_uri, echo=True)
    con = engine.connect()
    df = pd.read_sql_query("""
       SELECT j.typeID, j.iconID, i.categoryID FROM Joined_InvTypes j
       join invGroups i on j.groupID = i.groupId
       WHERE i.categoryID = 6

       """, con)

    con.close()
    print(len(df))

if __name__ == "__main__":
    SDEsql = '../ESI_Utilities/SDE/SDE sqlite-latest.sqlite'
    mysql_uri = f"mysql+pymysql://{fit_sqlfile}"
    SDE_uri = f"sqlite:///{SDEsql}"

    engine = create_engine(SDE_uri, echo=True)
    con = engine.connect()
    msg = """
    SELECT t.typeID, t.iconID, g.groupID, g.categoryID  FROM invGroups g
    JOIN invTypes t ON t.groupID = g.groupId
    where g.categoryID = 6
    
    """
    df = pd.read_sql_query(msg, con)
    df.to_csv("missing_icons.csv")

    # df = pd.read_sql_query("""
    #     SELECT j.typeID, j.iconID, j.groupID, i.categoryID FROM invTypes j
    #     join invGroups i on j.groupID = i.groupId
    #     WHERE i.categoryID = 6 AND j.iconID is NOT NULL
    #
    #     """, con)

    con.close()
    print(len(df))
    print(df.head())
