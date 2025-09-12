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
    
@app.get("/load_in_all_data")
def load_in_all_data():
    try:
        query = f"""
            SELECT [ID], [Auth Site], [Auth Number], [Start Date], [Exp Date], [State],
                   [Sender], [NEPM], [Phys State], [Tonnage Initial],
                   [Tonnage Remaining], [Generator], [Responsible]
            FROM [Register].[Incoming] 
            ORDER BY [Exp Date] ASC
        """
        result = run_sql(query)
        return result
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
    
@app.get("/load_out_data")
def load_out_data():
    try:
        today = date.today().strftime("%Y/%m/%d")
        query = f"""
            SELECT [ID], [Auth Site], [Auth Number], [Start Date], [Exp Date], [State],
                   [Receiver], [NEPM], [Phys State], [Tonnage Initial],
                   [Tonnage Remaining], [Generator], [Responsible]
            FROM [Register].[Outgoing] 
            WHERE ([Exp Date] >= '{today}') AND ([Tonnage Remaining] > '0') 
            ORDER BY [Exp Date] ASC
        """
        result = run_sql(query)
        return result
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
    
@app.get("/load_out_all_data")
def load_out_all_data():
    try:
        query = f"""
            SELECT [ID], [Auth Site], [Auth Number], [Start Date], [Exp Date], [State],
                   [Receiver], [NEPM], [Phys State], [Tonnage Initial],
                   [Tonnage Remaining], [Generator], [Responsible]
            FROM [Register].[Outgoing] 
            ORDER BY [Exp Date] ASC
        """
        result = run_sql(query)
        return result
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/load_wtc")
def load_wtc():
    try:
        today = date.today().strftime("%Y/%m/%d")
        query = f"""
            SELECT * FROM [Register].[WTC] ORDER BY [Use Date] ASC
        """
        result = run_sql(query)
        return result
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# === Enable upload/download abilities ===

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
    
@app.post("/insert_out_data")
async def insert_out_data(request: Request):
    try:
        data = await request.json()

        # Example fields from frontend
        auth_site = data.get("Auth Site")
        auth_number = data.get("Auth Number")
        start_date = data.get("Start Date")
        exp_date = data.get("Exp Date")
        state = data.get("State")
        receiver = data.get("customer")
        receiver_license = data.get("cust_lic")
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
            INSERT INTO [Register].[Outgoing] 
            ([Auth Site], [Auth Number], [Start Date], [Exp Date], [State],
             [Receiver],[License],[Detination],[Transporter],[Transporter License],[NEPM],[Description],[Phys State], [Tonnage Initial],
             [Tonnage Remaining], [Generator], [Responsible])
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        with pyodbc.connect(conn_str, autocommit=True) as conn:
            cursor = conn.cursor()
            cursor.execute(query, (
                auth_site, auth_number, start_date, exp_date, state,
                receiver, receiver_license, destination, transporter, trans_lic, nepm, description,
                phys_state, tonnage_initial, tonnage_remaining, generator, responsible
            ))

        return {"status": "inserted"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/wtc_data")
async def wtc_data(request: Request):
    try:
        data = await request.json()

        # Example fields from frontend
        auth_site = data.get("auth_site")
        auth_number = data.get("auth_num")
        ship_date = data.get("ship_date")
        use_date = data.get("use_date")
        direction = data.get("direction")
        wtcqld = data.get("wtcqld")
        wtcext = data.get("wtcext")
        nepm = data.get("nepm")
        tonnage = data.get("ton")
        responsible = data.get("user")

        query = f"""UPDATE [Register].[{direction}] 
            SET [Tonnage Remaining] = [Tonnage Remaining] - {tonnage}
            WHERE [Auth Number]='{auth_number}';
            INSERT INTO [Register].[Outgoing] 
            ([Auth Site], [Auth Number], [Shipping Date], [Use Date], [Incoming/Outgoing],
             [WTC QLD], [WTC EXT], [NEPM], [Tonnage], [Authorised By])
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        with pyodbc.connect(conn_str, autocommit=True) as conn:
            cursor = conn.cursor()
            cursor.execute(query, (
                auth_site, auth_number, ship_date, use_date, direction,
                wtcqld, wtcext, nepm, tonnage, responsible
            ))

        return {"status": "inserted"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ---------- ENDPOINTS ----------
@app.get("/health")
def health_check():
    try:
        with pyodbc.connect(conn_str, autocommit=True) as conn:
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
