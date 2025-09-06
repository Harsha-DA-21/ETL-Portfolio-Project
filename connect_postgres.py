# connect_postgres.py
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path=".env")

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

def get_connection():
    """Return a PostgreSQL connection"""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )


























































# import psycopg2
# import pandas as pd
# import os
# from dotenv import load_dotenv

# # Load environment variables
# load_dotenv()

# # Fetch credentials
# PG_HOST = os.getenv("PG_HOST")
# PG_PORT = os.getenv("PG_PORT")
# PG_DB = os.getenv("PG_DB")
# PG_USER = os.getenv("PG_USER")
# PG_PASSWORD = os.getenv("PG_PASSWORD")

# # Connect to PostgreSQL
# try:
#     conn = psycopg2.connect(
#         host=PG_HOST,
#         port=PG_PORT,
#         database=PG_DB,
#         user=PG_USER,
#         password=PG_PASSWORD
#     )
#     print("✅ Connection successful!")

#     # Create cursor
#     cursor = conn.cursor()

#     # Check PostgreSQL version
#     # cursor.execute("SELECT version();")
#     # record = cursor.fetchone()
#     # print("PostgreSQL version:", record)

#     # # Example: Read data from one table
#     # df = pd.read_sql("SELECT * FROM customer_mgmt.customers LIMIT 5;", conn)
#     # print("\nSample Data from customer_mgmt.customers:")
#     # print(df)

#     cursor.close()
#     conn.close()


# except Exception as e:
#     print("❌ Error while connecting:", e)
