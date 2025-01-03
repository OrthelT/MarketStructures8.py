from datetime import datetime
from typing import Optional

from sqlalchemy import (
    String,
    Integer,
    Float,
    DateTime,
    Boolean,
    PrimaryKeyConstraint,
)
from sqlalchemy.orm import declarative_base, mapped_column, Mapped

Base = declarative_base()

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


class ShipsDestroyed(Base):
    __tablename__ = "ShipsDestroyed"
    type_id: Mapped[int] = mapped_column(Integer)
    group_id: Mapped[int] = mapped_column(Integer)
    kill_time: Mapped[datetime] = mapped_column(DateTime)
    type_name: Mapped[str] = mapped_column(String(100))
    character_id: Mapped[int] = mapped_column(Integer)
    killmail_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime)


class DoctrineTargets(Base):
    __tablename__ = "DoctrinesTargets"
    fit_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    fit_name: Mapped[str] = mapped_column(String(100))
    type_name: Mapped[Optional[str]] = mapped_column(String(100))
    target_stock: Mapped[int] = mapped_column(Integer)
    ship_losses: Mapped[int] = mapped_column(Integer)
    adj_target: Mapped[int] = mapped_column(Integer)

    if __name__ == '__main__':
        pass
