import polars as pl

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
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, foreign

sql_file = "market_orders.sqlite"

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


class Doctrines(Base):
    __tablename__ = "Doctrines"
    doctrine_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    doctrine_name: Mapped[str] = mapped_column(String(100))
    doctrine_version: Mapped[str] = mapped_column(String(100))


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


class ShipMetadata:
    ALLOWED_SHIP_ROLES = {"dps", "logi", "links", "tackle", "ewar"}

    def __init__(self, ship_role):
        self.ship_role = ship_role

    @property
    def ship_role(self):
        return self._ship_role

    @ship_role.setter
    def ship_role(self, value):
        if value not in self.ALLOWED_SHIP_ROLES:
            raise ValueError(
                f"ValueError: Ship roles can only include the following: {', '.join(self.ALLOWED_SHIP_ROLES)}"
            )
        self._ship_role = value


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
    return df


def initialize_database(engine, base):
    """Create all database tables."""
    base.metadata.create_all(engine)


def update_current_orders(df: pl.DataFrame) -> str:
    df_processed = insert_type_names(df)
    records = df_processed.to_dicts()

    engine = create_engine(f"sqlite:///{sql_file}", echo=False)
    batch_size = 1000
    status = "failed"
    current_statement = """
        INSERT INTO current_orders
            (order_id, type_id, type_name, volume_remain, price, issued, duration, is_buy_order, timestamp)
            VALUES
            (:order_id, :type_id, :type_name, :volume_remain, :price, :issued, :duration, :is_buy_order, :timestamp);
        """

    clear_table = """
        DELETE FROM current_orders;
            """
    with engine.begin() as conn:

        try:
            conn.execute(text(clear_table))
        except Exception as e:
            print(f"Error clearing table: {str(e)}")
            raise
        print("table cleared")

        try:
            for i in range(0, len(records), batch_size):
                batch = records[i: i + batch_size]
                conn.execute(text(current_statement), batch)
                print(
                    f"\rProcessed records {i} to {min(i + batch_size, len(records))}",
                    end="",
                )
            status = "Data loading completed successfully!"
        except Exception as e:
            print(f"Error inserting data: {str(e)}")
            raise

    return status


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


if __name__ == "__main__":
    pass
