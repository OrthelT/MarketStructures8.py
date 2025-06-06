
from datetime import datetime, timezone

import pandas as pd
import polars as pl
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy.orm import sessionmaker

from sql_handler import fit_sqlfile, read_history
from sql_handler import insert_pd_timestamp
from sql_handler import mkt_sqlfile
from sql_handler import sql_file
from sql_handler import sql_logger


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


# def read_short_items() -> pd.DataFrame:
#     engine = create_engine(f"sqlite:///{sql_file}", echo=False)
#     df = pd.read_sql_query("SELECT * FROM ShortItems", engine)
#     engine.dispose()
#     print(f'connection closed: {engine}...returning orders from ShortItems table.')
#
#     return df


def insert_timestamp(df: pl.DataFrame) -> pl.DataFrame:
    ts = datetime.now(timezone.utc)
    df = df.with_columns(
        pl.lit(ts).alias("timestamp"),
    )
    return df


#
# class Doctrine_Fits(Base):
#     __tablename__ = "Doctrine_Fittings"
#     doctrine_id: Mapped[int] = mapped_column(Integer)
#     doctrine_name: Mapped[str] = mapped_column(String(100))
#     doctrine_version: Mapped[str] = mapped_column(String(100))
#     fitting_name: Mapped[str] = mapped_column(String(100), primary_key=True)
#     ship_class: Mapped[str] = mapped_column(String(50))
#     ship_group: Mapped[str] = mapped_column(String(50))
#     ship_group_id: Mapped[str] = mapped_column(String(100))
#
#
# class Fitting_Items(Base):
#     __tablename__ = "Fittings"
#     type_id: Mapped[str] = mapped_column(String(10), primary_key=True)
#     fitting_name: Mapped[str] = mapped_column(String(100))
#     type_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
#     qty_required: Mapped[int] = mapped_column(Integer)
#     fitting_version: Mapped[int] = mapped_column(Integer)
#     is_hull: Mapped[bool] = mapped_column(Boolean)
#
# class ShortItems(Base):
#     __tablename__ = "ShortItems"
#     id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
#     fit_id: Mapped[int] = mapped_column(Integer)
#     doctrine_name: Mapped[str] = mapped_column(String(100), nullable=True)
#     type_id: Mapped[int] = mapped_column(Integer)
#     type_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
#     quantity: Mapped[int] = mapped_column(Integer, nullable=True)
#     volume_remain: Mapped[float] = mapped_column(Float, nullable=True)
#     price: Mapped[float] = mapped_column(Float, nullable=True)
#     fits_on_market: Mapped[int] = mapped_column(Integer, nullable=True)
#     delta: Mapped[float] = mapped_column(Float, nullable=True)
#     doctrine_id: Mapped[int] = mapped_column(Integer, nullable=True)
#     ship_type_id: Mapped[int] = mapped_column(Integer, nullable=True)
#     timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=True)
#
# class CurrentOrders(Base):
#     __tablename__ = "current_orders"
#     order_id: Mapped[int] = mapped_column(primary_key=True)
#     type_id: Mapped[str] = mapped_column(String(10))
#     type_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
#     volume_remain: Mapped[int] = mapped_column(Integer)
#     price: Mapped[float] = mapped_column(Float)
#     issued: Mapped[datetime] = mapped_column(DateTime)
#     duration: Mapped[int] = mapped_column(Integer)
#     is_buy_order: Mapped[bool] = mapped_column(Boolean)
#     timestamp: Mapped[datetime] = mapped_column(DateTime)
#

# class Doctrine_Items(Base):
#     __tablename__ = "Doctrine_Items"
#     id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
#     fit_id: Mapped[int] = mapped_column(Integer)
#     doctrine_name: Mapped[str] = mapped_column(String(100), nullable=True)
#     type_id: Mapped[int] = mapped_column(Integer)
#     type_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
#     quantity: Mapped[int] = mapped_column(Integer, nullable=True)
#     volume_remain: Mapped[float] = mapped_column(Float, nullable=True)
#     price: Mapped[float] = mapped_column(Float, nullable=True)
#     fits_on_market: Mapped[int] = mapped_column(Integer, nullable=True)
#     delta: Mapped[float] = mapped_column(Float, nullable=True)
#     doctrine_id: Mapped[int] = mapped_column(Integer, nullable=True)
#     ship_type_id: Mapped[int] = mapped_column(Integer, nullable=True)
#     timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=True)

# class MarketBasket(Base):
#     __tablename__ = "MarketBasket"
#
#     type_id: Mapped[int] = mapped_column(Integer)
#     date: Mapped[datetime] = mapped_column(DateTime)
#     buy_price_low: Mapped[float] = mapped_column(Float)
#     buy_price_avg: Mapped[float] = mapped_column(Float)
#     buy_price_high: Mapped[float] = mapped_column(Float)
#     sell_price_low: Mapped[float] = mapped_column(Float)
#     sell_price_avg: Mapped[float] = mapped_column(Float)
#     sell_price_high: Mapped[float] = mapped_column(Float)
#     buy_volume_low: Mapped[int] = mapped_column(Integer)
#     buy_volume_avg: Mapped[int] = mapped_column(Integer)
#     buy_volume_high: Mapped[int] = mapped_column(Integer)
#     sell_volume_low: Mapped[int] = mapped_column(Integer)
#     sell_volume_avg: Mapped[int] = mapped_column(Integer)
#     sell_volume_high: Mapped[int] = mapped_column(Integer)
#     ore: Mapped[str] = mapped_column(String(100))
#     qty: Mapped[int] = mapped_column(Integer)
#
#     __table_args__ = (PrimaryKeyConstraint("date", "type_id"),)
# def clean_doctrine_columns(df: pd.DataFrame) -> pd.DataFrame:
#     # df = df.drop(columns=["doctrine_id", "ship_type_id"])
#     doctrines = get_doctrine_fits()
#     doctrines = doctrines.rename(columns={'name': 'doctrine_name', 'id': 'fit_id'})
#     doctrines.drop('type_name', inplace=True, axis=1)
#
#     merged_df = df.merge(doctrines, on='doctrine_name', how='left')
#
#     new_cols = ['type_id', 'type_name', 'quantity', 'volume_remain', 'price', 'fits_on_market',
#                 'delta', 'fit_id', 'doctrine_name', 'doctrine_id', 'ship_type_id']
#     updated_merged_df = merged_df[new_cols]
#     return updated_merged_df


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
    missing_df['total_volume_remain'] = stats['total_volume_remain']

    # fill historical values where available
    hist = read_history(30)
    hist_grouped = hist.groupby("type_id").agg({'average': 'mean', 'volume': 'mean'})
    missing_df['avg_of_avg_price'] = missing_df['type_id'].map(hist_grouped['average'])
    missing_df['avg_daily_volume'] = missing_df['type_id'].map(hist_grouped['volume'])

    # all null values must die
    missing_df = missing_df.infer_objects()
    missing_df = missing_df.fillna(0)

    # put timestamps back in because SQL Alchemy will very cross with us
    # if we put zeros in the timestamp column while nuking the null values
    # datetime values can never be 0
    missing_df['timestamp'] = stats['timestamp']

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
        sql_logger.error(f"Error occurred: {str(e)}")
        raise


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

def update_hist_expanded_group_category():
    statement1 = """
    UPDATE history_expanded
    SET
        category_id = JoinedInvTypes.categoryID,
        category_name = JoinedInvTypes.categoryName,
        group_id = JoinedInvTypes.groupID,
        group_name = JoinedInvTypes.groupName
    FROM
        JoinedInvTypes
    WHERE
        history_expanded.type_id = JoinedInvTypes.typeID;
    """

def plot_ship_volume(ship_group_id):
    engine = create_engine(mkt_sqlfile, echo=False)
    statement1 = f"""
      SELECT * FROM market_history
      WHERE market_history.category_id = 6
      AND market_history.group_id = {ship_group_id}
      AND market_history.date > 2024-08-01;
      """
    with engine.connect() as conn:
        df = pd.read_sql_query(statement1, conn)

    # Ensure 'date' is in datetime format
    df['date'] = pd.to_datetime(df['date'])

    ship_name = df[df['group_id'] == ship_group_id]['group_name'].unique()[0]
    print(ship_name)
    # Filter for Battleships
    ship_df = df[df['group_id'] == ship_group_id]
    ship_df = ship_df.sort_values(by='date')
    ship_df = ship_df.reset_index(drop=True)
    ship_df = ship_df[ship_df['date'] > '2024-01-01']
    ship_df = ship_df[ship_df['type_id'] == 17732]
    # Group by date and sum volumes
    daily_volume = ship_df.groupby(['date', 'type_name'])['volume'].sum().reset_index()

    pivoted = daily_volume.pivot(index='date', columns='type_name', values='volume')

    # Plot the data
    plt.figure(figsize=(15, 6))
    pivoted.plot(kind='line', marker='o', title=f'Daily Volume for {ship_name}')
    plt.xlabel('Date')
    plt.ylabel('Volume')
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    engine.dispose()


def get_doctrines_on_market() -> pd.DataFrame:
    df = get_doctrine_status_optimized()

    reordered_cols = ['fit id', 'type id', 'category', 'fit', 'ship', 'item', 'qty', 'stock', 'fits',
                      'days', '4H price', 'avg vol', 'avg price', 'delta', 'doctrine', 'group', 'cat id',
                      'grp id', 'doc id', 'ship id', 'timestamp']

    cols = ['fit_id', 'type_id', 'category', 'fit', 'ship', 'item', 'qty', 'stock', 'fits', 'days', 'price_4h',
            'avg_vol', 'avg_price', 'delta', 'doctrine', 'group', 'cat_id', 'grp_id', 'doc_id', 'ship_id', 'timestamp']

    colszip = zip(reordered_cols, cols)
    df.rename(columns=dict(colszip), inplace=True)

    df2 = df[['fit_id', 'fit', 'ship', 'type_id', 'item', 'stock', 'fits', 'price_4h',
              'avg_vol', 'avg_price', 'delta', 'timestamp', 'doctrine', 'group', 'cat_id',
              'grp_id', 'doc_id', 'ship_id', 'timestamp']]

    fit_counts = df2.groupby('type_id')['fit'].transform('count')
    df2['fit_counts'] = fit_counts

    return df2

    return None


#
# def ps_qty():
#     df2 = df[df['flag'].apply(lambda x: search(digit_end, x) is not None)]
#     df3 = df2.loc[9:11]
#
#     df4 = df3.copy()
#     pd.set_option('display.max_columns', None)
#
#     df4.loc[0, 'flag'] = 'HiSlot1'
#     df4 = df4.reset_index(drop=True)
#     df4.drop(df4.index[3], inplace=True)
#
#     for i in range(4, 8):
#         df4.loc[i] = df4.loc[0]
#         df4.loc[i, 'flag'] = f'HiSlot{i - 2}'
#
#     df4[['type_id', 'quantity', 'fit_id', 'type_fk_id']] = df4[['type_id', 'quantity', 'fit_id', 'type_fk_id']].astype(
#         int)
#
#     df4 = df4[df4.type_id == 14278]
#     df4 = df4.reset_index(drop=True)
#
#     df.drop(index=9, inplace=True)
#     df.reset_index(drop=True, inplace=True)
#
#     df5 = pd.concat([df, df4]).reset_index(drop=True)
#     print(df5)

if __name__ == "__main__":
    pass