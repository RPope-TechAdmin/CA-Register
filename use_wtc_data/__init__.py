import logging
import azure.functions as func
import pymssql
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
        direction = data.get("direction")   # "Incoming" or "Outgoing"
        wtcqld = data.get("wtcqld")
        wtcext = data.get("wtcext")
        nepm = data.get("nepm")
        tonnage = data.get("tonnage")
        responsible = data.get("applicant")

        # === Connect with pymssql ===
        conn = pymssql.connect(
            server=os.getenv("SQL_SERVER"),
            user=os.getenv("SQL_USERNAME"),
            password=os.getenv("SQL_PASSWORD"),
            database=os.getenv("SQL_DATABASE"),
            port=1433
        )
        cursor = conn.cursor()

        # === Insert into WTC table ===
        query_insert = f"""
            INSERT INTO [Register].[WTC] 
            ([Auth Site], [Auth Number], [Shipping Date], [Use Date], [Incoming/Outgoing],
             [WTC QLD], [WTC Ext], [NEPM], [Tonnage], [Authorised By])
             VALUES ({auth_site}, {auth_number}, {ship_date}, {use_date}, {direction}, {wtcqld}, {wtcext}, {nepm} {tonnage}, {responsible})
        """
        logging.info(f"Insert Query: {query_insert}")
        cursor.execute(query_insert)

        # === Update the corresponding Incoming/Outgoing table ===
        query_update = f"""
            UPDATE [Register].[{direction}]
            SET [Tonnage Remaining] = [Tonnage Remaining] - {tonnage}
            WHERE [Auth Number] = {auth_number} AND [NEPM] = {nepm}
        """
        
        logging.info(f"Alter Query: {query_update}")
        cursor.execute(query_update)

        conn.commit()
        conn.close()

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
