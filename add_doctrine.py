from sqlalchemy import create_engine, MetaData, select, insert

fit_mysqlfile = "mysql+pymysql://Orthel:Dawson007!27608@localhost:3306/wc_fitting"


def copy_row(doctrine_id: int):
    """
    Copy a row from one table to another with the same schema.

    Args:
        source_table_name (str): Name of the source table
        target_table_name (str): Name of the target table
        condition_column (str): Column name to use in the WHERE condition
        condition_value: Value to match in the condition column
    """

    # Database connection string - replace with your actual credentials
    connection_string = fit_mysqlfile

    # Create engine and connect
    engine = create_engine(connection_string)
    connection = engine.connect()

    try:
        # Get metadata
        metadata = MetaData()
        metadata.reflect(bind=engine)

        # Get table objects
        source_table = metadata.tables["fittings_doctrine"]
        target_table = metadata.tables["watch_doctrines"]

        names = source_table.columns.keys()
        print(names)

        select_stmt = select(source_table).where(source_table.c.id == doctrine_id)

        insert_stmt = insert(target_table).from_select(
            ["id", "name", "icon_url", "description", "created", "last_updated"], select_stmt)
        connection.execute(insert_stmt)
        connection.commit()
        print("Row copied successfully!")
        connection.close()


    except Exception as e:
        print(f"Error occurred: {str(e)}")


# Example usage for your specific case
if __name__ == "__main__":
    copy_row(

        doctrine_id=63
    )
