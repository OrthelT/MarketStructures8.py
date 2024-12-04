
import json
import polars as pl
from datetime import datetime
from typing import List, Optional

from sqlalchemy import create_engine, ForeignKey, String, MetaData, Table, Column, Integer, Float, DateTime, Boolean, \
    PrimaryKeyConstraint, text
from sqlalchemy.orm import DeclarativeBase, Session, Mapped, mapped_column, relationship

sql_file = 'market_orders.sqlite'

market_columns = [
    'type_id',
    'volume_remain',
    'price',
    'issued',
    'duration',
    'order_id',
    'is_buy_order'
]
history_columns = [
    'date',
    'type_id',
    'average',
    'highest',
    'lowest',
    'order_count',
    'volume']


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

def process_esi_market_order(data: dict, is_history: Boolean = False) -> dict:
    engine = create_engine(f"sqlite:///{sql_file}", echo=False)
    initialize_database(engine, Base)

    if is_history:
        columns = history_columns
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
    else:
        columns = market_columns
        stmt = """
        INSERT INTO market_order 
            (order_id, type_id, volume_remain, price, issued, duration, is_buy_order)
        VALUES 
            (:order_id, :type_id, :volume_remain, :price, :issued, :duration, :is_buy_order)
        ON CONFLICT(order_id) DO UPDATE SET
            volume_remain = EXCLUDED.volume_remain,
            price = EXCLUDED.price,
            issued = EXCLUDED.issued,
            duration = EXCLUDED.duration,
            is_buy_order = EXCLUDED.is_buy_order
        """

    df = pl.DataFrame(data)
    df_processed = process_dataframe(df, columns)
    records = df_processed.to_dicts()

    batch_size = 1000
    status = "failed"

    with engine.begin() as conn:
        try:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                conn.execute(text(stmt), batch)
                print(f"Processed records {i} to {min(i + batch_size, len(records))}")
            status = "Data loading completed successfully!"
        except Exception as e:
            print(f"Error inserting data: {str(e)}")
            raise

    return status


if __name__ == '__main__':
    print('Starting...')
    #
    # print(type(history_data))
    # process_esi_market_order(data=history_data, is_history=True)
