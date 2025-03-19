from datetime import datetime
from typing import Optional

from sqlalchemy import (
    String,
    Integer,
    Float,
    DateTime,
    Boolean,
    PrimaryKeyConstraint, BigInteger)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

mkt_sqlfile = "sqlite:///market_orders.sqlite"
testdb = 'sqlite:///test.sqlite'
fit_mysqlfile = "mysql+pymysql://Orthel:Dawson007!27608@localhost:3306/wc_fitting"

Base = DeclarativeBase()


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

class Doctrines(Base):
    __tablename__ = "Doctrines"
    fit_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    type_id: Mapped[int] = mapped_column(Integer)
    category: Mapped[str] = mapped_column(String(10))
    fit: Mapped[str] = mapped_column(String(100))
    ship: Mapped[str] = mapped_column(String(100))
    item: Mapped[str] = mapped_column(String(100))
    qty: Mapped[int] = mapped_column(Integer)
    stock: Mapped[int] = mapped_column(Integer)
    fits: Mapped[float] = mapped_column(Float)
    days: Mapped[float] = mapped_column(Float)
    price_4h: Mapped[float] = mapped_column(Float)
    avg_vol: Mapped[float] = mapped_column(Float)
    avg_price: Mapped[float] = mapped_column(Float)
    delta: Mapped[float] = mapped_column(Float)
    doctrine: Mapped[str] = mapped_column(String(100))
    group: Mapped[str] = mapped_column(String(100))
    cat_id: Mapped[int] = mapped_column(Integer)
    grp_id: Mapped[int] = mapped_column(Integer)
    doc_id: Mapped[int] = mapped_column(Integer)
    ship_id: Mapped[int] = mapped_column(Integer)
    timestamp: Mapped[datetime] = mapped_column(DateTime)

class DataMaps(Base):
    __tablename__ = "data_maps"
    data_instance: Mapped[str] = mapped_column(String(100), primary_key=True)
    type_id: Mapped[int] = mapped_column(Integer, nullable=True)
    type_name: Mapped[Optional[str]] = mapped_column(String(100))
    stock: Mapped[int] = mapped_column(Integer, nullable=True)
    min_price: Mapped[float] = mapped_column(Float, nullable=True)
    price: Mapped[float] = mapped_column(Float, nullable=True)
    avg_price: Mapped[float] = mapped_column(Float, nullable=True)
    avg_volume: Mapped[float] = mapped_column(Float, nullable=True)
    group: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    group_id: Mapped[str] = mapped_column(String(10))
    category: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    category_id: Mapped[str] = mapped_column(String(10))
    days: Mapped[int] = mapped_column(Integer, nullable=True)
    fit_id: Mapped[int] = mapped_column(Integer, nullable=True)
    fits: Mapped[float] = mapped_column(Float, nullable=True)
    fit: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    doctrine: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    doctrine_id: Mapped[int] = mapped_column(Integer, nullable=True)
    ship: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    ship_id: Mapped[int] = mapped_column(Integer, nullable=True)
    qty: Mapped[int] = mapped_column(Integer, nullable=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime)

class JoinedInvTypes(Base):
    __tablename__ = "JoinedInvTypes"
    typeId: Mapped[int] = mapped_column(Integer, primary_key=True)
    groupID: Mapped[int] = mapped_column(Integer)
    typeName: Mapped[str] = mapped_column(String(100))
    groupName: Mapped[Optional[str]] = mapped_column(String(100))
    categoryID: Mapped[int] = mapped_column(Integer)
    categoryName: Mapped[Optional[str]] = mapped_column(String(100))
    metaGroupID: Mapped[int] = mapped_column(Integer)
    metaGroupName: Mapped[Optional[str]] = mapped_column(String(100))

class Fittings_FittingItem(Base):
    __tablename__ = "fittings_fittingitem"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    flag: Mapped[str] = mapped_column(String(25))
    quantity: Mapped[int] = mapped_column(Integer)
    type_id: Mapped[int] = mapped_column(Integer)
    fit_id: Mapped[int] = mapped_column(Integer)
    type_fk_id: Mapped[int] = mapped_column(BigInteger)

if __name__ == '__main__':
    pass