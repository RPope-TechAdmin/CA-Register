import pyodbc
import os

conn_str = (
    f"Driver={{ODBC Driver 17 for SQL Server}};"
    f"Server={os.getenv('SQL_SERVER')};"
    f"Database={os.getenv('SQL_DATABASE')};"
    f"Uid={os.getenv('SQL_USER')};"
    f"Pwd={os.getenv('SQL_PASSWORD')};"
    "Encrypt=yes;TrustServerCertificate=no;"
)
conn = pyodbc.connect(conn_str, autocommit=True)
