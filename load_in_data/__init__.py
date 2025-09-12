import os
import pyodbc
from datetime import date, time
from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

app = FastAPI()

# === CONNECTION STRING VARIABLES ===
SQL_SERVER = "purenvqld.database.windows.net"
SQL_DATABASE = "ConsignmentsQLD"
SQL_USERNAME = "CARegister"
SQL_PASSWORD = "C4R3g1s73r"

conn_str = (
        r"Driver={ODBC Driver 17 for SQL Server};"
        f"Server=tcp:{SQL_SERVER},1433;"
        f"Database={SQL_DATABASE};"
        f"Uid={SQL_USERNAME};"
        f"Pwd={SQL_PASSWORD};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )

# === SQL CONNECTION STRING ===
def run_sql(query: str):
    with pyodbc.connect(conn_str, autocommit=True) as conn:
        cursor = conn.cursor()
        cursor.execute(query)

        if cursor.description:  # SELECT query
            cols = [c[0] for c in cursor.description]
            rows = [list(r) for r in cursor.fetchall()]
            return {"columns": cols, "rows": rows}
        else:
            return {"status": "ok"}

# === Load Data Tabels ===
@app.get("/load_in_data")
def load_in_data():
    try:
        today = date.today().strftime("%Y/%m/%d")
        query = f"""
            SELECT [ID], [Auth Site], [Auth Number], [Start Date], [Exp Date], [State],
                   [Sender], [NEPM], [Phys State], [Tonnage Initial],
                   [Tonnage Remaining], [Generator], [Responsible]
            FROM [Register].[Incoming] 
            WHERE ([Exp Date] >= '{today}') AND ([Tonnage Remaining] > '0') 
            ORDER BY [Exp Date] ASC
        """
        result = run_sql(query)
        return result
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})