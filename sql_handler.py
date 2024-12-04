import sqlite3
import os
import json
import polars as ps
from datetime import datetime
from typing import List
from typing import Optional

import sqlalchemy
from sqlalchemy import ForeignKey, Engine
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase, Session
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, Integer, String

sql_file = 'market_orders.sqlite'
data_path = 'data/mkt_orders_raw.json'
new_cols = ['type_id', 'volume_remain', 'price', 'issued', 'duration',, 'is_buy_order']

class Base(DeclarativeBase):
    pass


class Market_Order(Base):
    __tablename__ = "market_order"
    order_id: Mapped[int] = mapped_column(primary_key=True)
    type_id: Mapped[str] = mapped_column(String(10))
    type_name: Mapped[Optional[str]]
    volume_remain: Mapped[int]
    price: Mapped[float]
    issued: Mapped[datetime]
    duration: Mapped[int]
    is_buy_order: Mapped[bool]


with open(data_path, 'r') as f:
    data = json.load(f)
data = data
df = ps.DataFrame(data)


def process_data(df: ps.DataFrame, new_columns: list = None, date_column_name: str = None):
    df = reorder_columns(df, new_columns)
    if date_column_name:
        df2 = convert_date(df, date_column_name)
        return df2
    else:
        print('No date column name provided')
        return df


def reorder_columns(df: ps.DataFrame, new_columns: list = None):
    if new_columns:
        print('reordering columns')
        df2 = df.select(new_cols)
        return df2
    else:
        print('no new column order selected')
        return df


def convert_date(df: ps.DataFrame, column_name: str):
    s = df[column_name].to_list()
    print(f'converting ISO formatted dates in {column_name} to datetime objects')
    new_dates = []
    for date in s:
        new_date = datetime.fromisoformat(date.replace("Z", "+00:00"))
        new_dates.append(new_date)
    new_dates = ps.Series('issued', new_dates)
    col_index: int = df.get_column_index(column_name)
    df2 = df.replace_column(col_index, new_dates)
    return df2


if __name__ == '__main__':
    new_cols = ['type_id', 'volume_remain', 'price', 'issued', 'duration', 'order_id', 'is_buy_order']
    old_cols = ['duration', 'is_buy_order', 'issued', 'location_id', 'min_volume', 'order_id', 'price', 'range',
                'type_id', 'volume_remain', 'volume_total']

    df = ps.DataFrame(data)
    df2 = process_data(df, new_cols, 'issued')

    data_dict = {}
    for item in data:
        data_dict = {
            'type_id': item['type_id'],
            'volume_remain': item['volume_remain'],
            'price': item['price'],
            'issued': item['issued'],
            'duration': item['duration'],
            'order_id': item['order_id'],
            'is_buy_order': item['is_buy_order']
        }
