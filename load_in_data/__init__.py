import logging
import azure.functions as func
import pymssql
import os
from datetime import date
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing load_in_data request.")

    # === CONNECTION VARIABLES (from Azure Function App Settings) ===
    sql_server = os.getenv("SQL_SERVER")
    sql_database = os.getenv("SQL_DATABASE")
    sql_username = os.getenv("SQL_USERNAME")
    sql_password = os.getenv("SQL_PASSWORD")

    today = date.today().strftime("%Y-%m-%d")

    query = f"""
        SELECT [ID], [Auth Site], [Auth Number], [Start Date], [Exp Date], [State],
               [Sender], [NEPM], [Phys State], [Tonnage Initial],
               [Tonnage Remaining], [Generator], [Responsible]
        FROM [Register].[Incoming] 
        WHERE [Exp Date] >= '{today}' AND [Tonnage Remaining] > 0
        ORDER BY [Exp Date] ASC
    """

    try:
        logging.info(f"Connecting to server={sql_server}, db={sql_database}, user={sql_username}")

        # Connect with pymssql
        conn = pymssql.connect(
            server=sql_server,
            user=sql_username,
            password=sql_password,
            database=sql_database,
            port=1433,
            as_dict=True  # fetch results as dicts
        )

        cursor = conn.cursor()
        cursor.execute(query, (today,))
        rows = cursor.fetchall()

        cols = list(rows[0].keys()) if rows else []
        logging.info(f"Query executed. {len(rows)} rows returned.")

        conn.close()

        return func.HttpResponse(
            body=json.dumps({"columns": cols, "rows": rows}, default=str),
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Database query failed: {e}")
        return func.HttpResponse(f"Error: {e}", status_code=500)
