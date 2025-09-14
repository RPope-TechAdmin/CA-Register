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
        SELECT * FROM [Register].[WTC]
        ORDER BY [Use Date] ASC
    """

    try:
        with pymssql.connect(server, username, password, database) as conn:
            cursor = conn.cursor(as_dict=True)
            cursor.execute(query, (today, 0))  # safe parameterization
            rows = cursor.fetchall()
            cols = list(rows[0].keys()) if rows else []

        return func.HttpResponse(
            body=json.dumps({"columns": cols, "rows": rows}, default=str),
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Database query failed: {e}")
        return func.HttpResponse(f"Error: {e}", status_code=500)
