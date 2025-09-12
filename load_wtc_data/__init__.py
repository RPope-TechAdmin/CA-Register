import logging
import azure.functions as func
import pyodbc
import os
from datetime import date
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing load_in_data request.")

    conn_str = (
        r"Driver={ODBC Driver 17 for SQL Server};"
        f"Server=tcp:{os.getenv('SQL_SERVER')},1433;"
        f"Database={os.getenv('SQL_DATABASE')};"
        f"Uid={os.getenv('SQL_USERNAME')};"
        f"Pwd={os.getenv('SQL_PASSWORD')};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )

    today = date.today().strftime("%Y/%m/%d")
    query = f"""
        SELECT * FROM [Register].[WTC] 
        ORDER BY [Use Date] ASC
    """

    try:
        with pyodbc.connect(conn_str, autocommit=True) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            cols = [c[0] for c in cursor.description]
            rows = [dict(zip(cols, r)) for r in cursor.fetchall()]

        return func.HttpResponse(
            body=json.dumps({"columns": cols, "rows": rows}),
            mimetype="application/json"
        )

    except Exception as e:
        return func.HttpResponse(f"Error: {e}", status_code=500)
