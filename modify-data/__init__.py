# backend.py
import os
import pyodbc
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse


app = FastAPI()

# === SQL CONNECTION STRING ===
CONN_STR = (
    "Driver={{ODBC Driver 17 for SQL Server}};"
    "Server=tcp:purenvqld.database.windows.net,1433;"
    "Database=ConsignmentsQLD;"
    "Uid=CARegister;"
    "Pwd=C4R3g1s73r;"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30"
)

def initial_conn():
    try:
        conn = pyodbc.connect(CONN_STR, autocommit=True)
        cursor = conn.cursor()
    except Exception as e:
        raise SystemExit(f"Could not connect to SQL: {e}")

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
