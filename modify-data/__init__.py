# backend.py
import os
import pyodbc
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse


app = FastAPI()

# === SQL CONNECTION STRING ===
CONN_STR = (
    f"Driver={{ODBC Driver 18 for SQL Server}};"
    f"Server={os.getenv('SQL_SERVER')};"
    f"Database={os.getenv('SQL_DATABASE')};"
    f"Uid={os.getenv('SQL_USER')};"
    f"Pwd={os.getenv('SQL_PASSWORD')};"
    "Encrypt=yes;TrustServerCertificate=no;"
)

def run_sql(query: str):
    with pyodbc.connect(CONN_STR, autocommit=True) as conn:
        cursor = conn.cursor()
        cursor.execute(query)

        if cursor.description:  # SELECT query
            cols = [c[0] for c in cursor.description]
            rows = [dict(zip(cols, r)) for r in cursor.fetchall()]
            return {"columns": cols, "rows": rows}
        else:  # INSERT/UPDATE/DELETE
            return {"status": "ok"}

# ---------- ENDPOINTS ----------
@app.get("/health")
def health_check():
    try:
        with pyodbc.connect(CONN_STR, autocommit=True) as conn:
            return {"status": "healthy"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "unhealthy", "error": str(e)})

@app.get("/query")
def query(q: str = Query(..., description="SQL query to execute")):
    try:
        result = run_sql(q)
        return result
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
