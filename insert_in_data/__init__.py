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

@app.post("/insert_in_data")
async def insert_in_data(request: Request):
    try:
        data = await request.json()

        # Example fields from frontend
        auth_site = data.get("Auth Site")
        auth_number = data.get("Auth Number")
        start_date = data.get("Start Date")
        exp_date = data.get("Exp Date")
        state = data.get("State")
        sender = data.get("customer")
        sender_license = data.get("cust_lic")
        destination = data.get("destination")
        transporter = data.get("transporter")
        trans_lic = data.get("trans_lic")
        nepm = data.get("NEPM")
        description = data.get("description")
        phys_state = data.get("phys_state")
        tonnage_initial = data.get("tonnage")
        tonnage_remaining = data.get("tonnage")
        generator = data.get("generator")
        responsible = data.get("applicant")

        query = """
            INSERT INTO [Register].[Incoming] 
            ([Auth Site], [Auth Number], [Start Date], [Exp Date], [State],
             [Sender],[License],[Detination],[Transporter],[Transporter License],[NEPM],[Description],[Phys State], [Tonnage Initial],
             [Tonnage Remaining], [Generator], [Responsible])
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        with pyodbc.connect(conn_str, autocommit=True) as conn:
            cursor = conn.cursor()
            cursor.execute(query, (
                auth_site, auth_number, start_date, exp_date, state,
                sender, sender_license, destination, transporter, trans_lic, nepm, description,
                phys_state, tonnage_initial, tonnage_remaining, generator, responsible
            ))

        return {"status": "inserted"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})