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

    query_incoming = """
        SELECT [Sender] FROM [Register].[Incoming] 
        ORDER BY [Exp Date] ASC
    """

    query_outgoing = """
        SELECT [Receiver] FROM [Register].[Outgoing] 
        ORDER BY [Exp Date] ASC
    """

    try:
        with pymssql.connect(server, username, password, database) as conn:
            cursor = conn.cursor(as_dict=True)
            cursor.execute(query_incoming, (today, 0))  # safe parameterization
            incoming_rows = [
                {"type": "Incoming", "number": row["number"], "partner": row["partner"]}
                for row in cursor.fetchall()
            ]
            cursor.execute(query_outgoing, (today, 0))
            outgoing_rows = [
                {"type": "Outgoing", "number": row["number"], "partner": row["partner"]}
                for row in cursor.fetchall()
            ]

            combined=incoming_rows + outgoing_rows

        return func.HttpResponse(
            body=json.dumps({"customers": combined}, default=str),
            mimetype="application/json",
            status_code=200,
        )

    except Exception as e:
        logging.error(f"Database query failed: {e}")
        return func.HttpResponse(f"Error: {e}", status_code=500)
