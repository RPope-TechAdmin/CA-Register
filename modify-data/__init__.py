# backend.py
import os
import pyodbc
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse


app = FastAPI()

# ========= CONFIG =========
# Read from Function App settings (Configuration → Application settings)
SQL_SERVER = "purenvqld.database.windows.net"
SQL_DATABASE = "ConsignmentsQLD"
SQL_USERNAME = "CARegister"
SQL_PASSWORD = "C4R3g1s73r"

# === SQL CONNECTION STRING ===
def connect_with_fallback(timeout_seconds: int = 60) -> pyodbc.Connection:
    """
    Try ODBC Driver 18 then 17. Increase Connection Timeout and retry a few times
    (useful if Azure SQL Serverless is resuming).
    """
    drivers = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]
    last_exc = None

    for driver in drivers:
        conn_str = (
            f"Driver={{{driver}}};"
            f"Server=tcp:{SQL_SERVER},1433;"
            f"Database={SQL_DATABASE};"
            f"Uid={SQL_USERNAME};"
            f"Pwd={SQL_PASSWORD};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            f"Connection Timeout={timeout_seconds};"
        )
        for attempt in range(3):
            try:
                return pyodbc.connect(conn_str)
            except Exception as e:
                last_exc = e
                logging.warning(f"Connect attempt {attempt+1}/3 with {driver} failed: {e}")
                time.sleep(3)
    # If we get here, all attempts failed
    raise last_exc
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
