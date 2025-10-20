import logging
import azure.functions as func
import pymssql
import os
from datetime import date
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing load_customers request.")

    # === CONNECTION VARIABLES FROM APP SETTINGS ===
    server = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DATABASE")
    username = os.getenv("SQL_USERNAME")
    password = os.getenv("SQL_PASSWORD")
    
    query_incoming = """
        SELECT DISTINCT [Sender] FROM [Register].[Incoming]
    """

    query_outgoing = """
        SELECT DISTINCT [Receiver] FROM [Register].[Outgoing]
    """

    try:
        with pymssql.connect(server, username, password, database) as conn:
            cursor = conn.cursor()
            
            # Fetch incoming customers
            cursor.execute(query_incoming)
            incoming_customers = {row[0] for row in cursor.fetchall() if row[0]}
            
            # Fetch outgoing customers
            cursor.execute(query_outgoing)
            outgoing_customers = {row[0] for row in cursor.fetchall() if row[0]}
            
            # Combine and sort unique customer names
            combined_customers = sorted(list(incoming_customers.union(outgoing_customers)))
            logging.info(f"Customers: {combined_customers}")

        return func.HttpResponse(
            body=json.dumps({"customers": combined_customers}, default=str),
            mimetype="application/json",
            status_code=200,
        )

    except Exception as e:
        logging.error(f"Database query failed: {e}")
        return func.HttpResponse(body=json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")
