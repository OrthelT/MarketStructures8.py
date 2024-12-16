import sys
import pandas as pd
import sqlalchemy
import mysql.connector


# print(f"Python version: {sys.version}")
# print(f"Pandas version: {pd.__version__}")
# print(f"SQLAlchemy version: {sqlalchemy.__version__}")
# print(f"MySQL Connector version: {mysql.connector.__version__}")

def test_minimal_connection():
    try:
        # First, try just creating the engine
        print("Step 1: Creating engine...")
        engine = sqlalchemy.create_engine(
            "mysql+mysqlconnector://Orthel:Dawson007!27608@localhost:3306/wc_fitting"
        )

        print("Step 2: Testing connection...")
        # Try to connect without executing any queries
        with engine.connect() as connection:
            print("Connection successful!")

        print("Step 3: Disposing engine...")
        engine.dispose()
        print("Test completed successfully!")

    except Exception as e:
        print(f"Error occurred at step: {str(e)}")


if __name__ == "__main__":
    test_minimal_connection()
