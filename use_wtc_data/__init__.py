import logging
import azure.functions as func
import pyodbc
import os
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing insert_in_data request.")

    try:
        # Parse JSON body from frontend
        data = req.get_json()

        auth_site = data.get("auth_site")
        auth_number = data.get("auth_num")
        ship_date = data.get("ship_date")
        use_date = data.get("use_date")
        direction = data.get("direction")
        wtcqld = data.get("wtcqld")
        wtcext = data.get("qtcext")
        nepm = data.get("nepm")
        tonnage = data.get("tonnage")
        responsible = data.get("applicant")


        conn_str = (
            r"Driver={ODBC Driver 17 for SQL Server};"
            f"Server=tcp:{os.getenv('SQL_SERVER')},1433;"
            f"Database={os.getenv('SQL_DATABASE')};"
            f"Uid={os.getenv('SQL_USERNAME')};"
            f"Pwd={os.getenv('SQL_PASSWORD')};"
            "Encrypt=yes;TrustServerCertificate=no;"
        )

        query = """
            INSERT INTO [Register].[WTC] 
            ([Auth Site], [Auth Number], [Ship Date], [Use Date], [Incoming/Outgoing],
             [WTC QLD], [WTC Ext], [NEPM], [Tonnage], [Responsible])
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        query_2= f"""
            UPDATE TABLE [Register].[{direction}]
            SET [Tonnage Remaining] = [Tonnage Remaining] - {tonnage}
            WHERE [Auth Number] = {auth_number} AND [NEPM] = {nepm} """

        with pyodbc.connect(conn_str, autocommit=True) as conn:
            cursor = conn.cursor()
            cursor.execute(query, (
                auth_site, auth_number, ship_date, use_date, direction,
                wtcqld, wtcext, nepm, tonnage, responsible))
            cursor.execute(query_2)

        return func.HttpResponse(
            body=json.dumps({"status": "inserted"}),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Insert failed: {e}")
        return func.HttpResponse(
            body=json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )