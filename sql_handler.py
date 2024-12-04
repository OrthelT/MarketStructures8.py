import sqlite3
import os
import json
import polars as pl
from datetime import datetime
from typing import List, Optional

from sqlalchemy import create_engine, ForeignKey, String, MetaData, Table, Column, Integer, Float, DateTime, Boolean, \
    PrimaryKeyConstraint
from sqlalchemy.orm import DeclarativeBase, Session, Mapped, mapped_column, relationship

sql_file = 'market_orders.sqlite'
data_path = 'data/mkt_orders_raw.json'
market_columns = [
    'type_id',
    'volume_remain',
    'price',
    'issued',
    'duration',
    'order_id',
    'is_buy_order'
]
history_columns = ['date', 'type_id', 'average', 'highest', 'lowest', 'order_count', 'volume']

history_path = 'data/master_history/all_history'


# with open(json_path, 'r') as f:
#     data = json.load(f)

class Base(DeclarativeBase):
    pass


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


class MarketHistory(Base):
    __tablename__ = "market_history"
    date: Mapped[datetime] = mapped_column(DateTime)
    type_id: Mapped[str] = mapped_column(String(10))
    average: Mapped[float] = mapped_column(Float)
    volume: Mapped[int] = mapped_column(Integer)
    highest: Mapped[float] = mapped_column(Float)
    lowest: Mapped[float] = mapped_column(Float)
    order_count: Mapped[int] = mapped_column(Integer)

    # Add composite primary key
    __table_args__ = (
        PrimaryKeyConstraint('date', 'type_id'),
    )


def process_dataframe(df: pl.DataFrame, columns: list, date_column: str = None) -> pl.DataFrame:
    """Process the dataframe by selecting columns and converting dates."""
    # Select only the specified columns
    df = df.select(columns)

    if date_column is None:
        if 'date' in df.columns:
            date_column = 'date'
        elif 'issued' in df.columns:
            date_column = 'issued'
        else:
            date_column = None

    # Convert  date strings to datetime objects
    if date_column:
        df = df.with_columns([
            pl.col(date_column).str.strptime(pl.Datetime).alias(date_column)
        ])

    return df


def initialize_database(engine, base):
    """Create all database tables."""
    base.metadata.create_all(engine)


def load_data_to_db(data: dict, db_path: str, columns: List[str], is_history: Boolean = False):
    """Load JSON data into SQLite database."""
    # Create SQLAlchemy engine
    engine = create_engine(f"sqlite:///{db_path}", echo=False)

    # Initialize database tables
    initialize_database(engine, Base)

    # Load and process data

    df = pl.DataFrame(data)
    df_processed = process_dataframe(df, columns)

    # Convert to records for insertion
    records = df_processed.to_dicts()

    # Insert data in batches
    batch_size = 1000
    with Session(engine) as session:
        try:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]

                if is_history:
                    market_orders = [MarketHistory(**record) for record in batch]
                else:
                    market_orders = [MarketOrder(**record) for record in batch]

                session.add_all(market_orders)
                session.commit()
                print(f"Inserted records {i} to {min(i + batch_size, len(records))}")
        except Exception as e:
            session.rollback()
            print(f"Error inserting data: {str(e)}")
            raise
        finally:
            session.close()


def load_market_history(json_path: str, db_path: str, columns: List[str]):
    engine = create_engine(f"sqlite:///{db_path}", echo=False)

    stmt = """
    INSERT INTO market_history 
        (date, type_id, average, volume, highest, lowest, order_count)
    VALUES 
        (:date, :type_id, :average, :volume, :highest, :lowest, :order_count)
    ON CONFLICT(date, type_id) DO UPDATE SET
        average = EXCLUDED.average,
        volume = EXCLUDED.volume,
        highest = EXCLUDED.highest,
        lowest = EXCLUDED.lowest,
        order_count = EXCLUDED.order_count
    """

    with open(json_path, 'r') as f:
        data = json.load(f)

    df = pl.DataFrame(data)
    df_processed = process_dataframe(df, columns)
    records = df_processed.to_dicts()

    batch_size = 1000
    with engine.begin() as conn:
        try:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                conn.execute(text(stmt), batch)
                print(f"Processed records {i} to {min(i + batch_size, len(records))}")
        except Exception as e:
            print(f"Error inserting data: {str(e)}")
            raise


def process_esi_market_order(data: dict, is_history: Boolean = False) -> dict:
    if is_history:
        columns = history_columns
    else:
        columns = market_columns

    try:
        load_data_to_db(data, sql_file, columns, is_history=is_history)
        status = "Data loading completed successfully!"
    except Exception as e:
        print(f"Failed to load data: {str(e)}")
        status = "failed"
    return status


if __name__ == '__main__':
    print('Starting...')
    #
    # print(type(history_data))
    # process_esi_market_order(data=history_data, is_history=True)
