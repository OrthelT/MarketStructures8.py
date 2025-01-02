import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy.orm import sessionmaker

from sql_handler import fit_sqlfile
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


def read_short_items() -> pd.DataFrame:
    engine = create_engine(f"sqlite:///{sql_file}", echo=False)
    df = pd.read_sql_query("SELECT * FROM ShortItems", engine)
    engine.dispose()
    print(f'connection closed: {engine}...returning orders from ShortItems table.')

    return df
