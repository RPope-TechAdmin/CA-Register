import logging
import azure.functions as func
import pymssql
import pandas as pd
import os
from io import BytesIO

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Generating Excel export")

    server = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DATABASE")
    username = os.getenv("SQL_USERNAME")
    password = os.getenv("SQL_PASSWORD")

    queries = {
        "Incoming": "SELECT * FROM [Register].[Incoming]",
        "Outgoing": "SELECT * FROM [Register].[Outgoing]",
        "Waste Tracking": "SELECT * FROM [Register].[WTC]"
    }

    try:
        conn = pymssql.connect(server, username, password, database)

        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            for sheet_name, query in queries.items():
                df = pd.read_sql(query, conn)
                df.to_excel(writer, sheet_name=sheet_name, index=False)

        conn.close()
        output.seek(0)

        return func.HttpResponse(
            body=output.read(),
            status_code=200,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=CA_Register.xlsx"
            }
        )

    except Exception as e:
        logging.error(str(e))
        return func.HttpResponse(
            f"Failed to generate Excel file: {e}",
            status_code=500
        )
