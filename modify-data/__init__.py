# backend.py
from fastapi import FastAPI, Query
import pyodbc
import os

app = FastAPI()

CONN_STR = (
    f"Driver={{ODBC Driver 18 for SQL Server}};"
    f"Server={os.getenv('SQL_SERVER')};"
    f"Database={os.getenv('SQL_DATABASE')};"
    f"Uid={os.getenv('SQL_USER')};"
    f"Pwd={os.getenv('SQL_PASSWORD')};"
    "Encrypt=yes;TrustServerCertificate=no;"
)

@app.get("/query")
def run_query(q: str = Query(..., description="SQL query to execute")):
    with pyodbc.connect(CONN_STR, autocommit=True) as conn:
        cursor = conn.cursor()
        cursor.execute(q)
        if cursor.description:  # SELECT queries
            cols = [c[0] for c in cursor.description]
            rows = [dict(zip(cols, r)) for r in cursor.fetchall()]
            return {"columns": cols, "rows": rows}
        return {"status": "ok"}
