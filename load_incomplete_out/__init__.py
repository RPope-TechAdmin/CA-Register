import logging
import azure.functions as func
import pymssql
import os
from datetime import date
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing load_in_data request.")

    # === CONNECTION VARIABLES FROM APP SETTINGS ===
    server = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DATABASE")
    username = os.getenv("SQL_USERNAME")
    password = os.getenv("SQL_PASSWORD")

    today = date.today()

    query = """
        SELECT [ID], [Auth Site], [Auth Number], [Start Date], [Exp Date], [State],
               [Receiver], [NEPM], [Phys State], [Tonnage Initial],
               [Tonnage Remaining], [Generator], [Responsible]
        FROM [Register].[Outgoing] 
        WHERE [Completion]= ''
        ORDER BY [Exp Date] ASC
    """

    try:
        with pymssql.connect(server, username, password, database) as conn:
            cursor = conn.cursor(as_dict=True)
            cursor.execute(query)
            rows = cursor.fetchall()

            # Ensure columns list is always available
            if rows:
                cols = list(rows[0].keys())
            else:
                # If no rows returned, fetch column names from description
                cursor2 = conn.cursor()
                cursor2.execute(query)
                cols = [desc[0] for desc in cursor2.description]
                cursor2.close()

        return func.HttpResponse(
            body=json.dumps({"columns": cols, "rows": rows}, default=str),
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Database query failed: {e}")
        return func.HttpResponse(f"Error: {e}", status_code=500)
