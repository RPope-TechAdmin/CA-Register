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
        state = data.get("state")
        sender = data.get("customer")
        nepm = data.get("nepm")
        tonnage = data.get("tonnage")
        direction = data.get("direction")

        
        
        # === pymssql connection (use env vars from Azure settings) ===
        conn = pymssql.connect(
            server=os.getenv("SQL_SERVER"),
            user=os.getenv("SQL_USERNAME"),
            password=os.getenv("SQL_PASSWORD"),
            database=os.getenv("SQL_DATABASE"),
            port=1433
        )
        if direction=="Incoming":
            query = """
                SELECT [ID], [Auth Site], [Auth Number], [Start Date], [Exp Date], [State],
                [Sender], [NEPM], [Phys State], [Tonnage Initial],
                [Tonnage Remaining], [Generator], [Responsible]
                FROM [Register].[Incoming]
                WHERE [Auth Site] = '%s' AND [NEPM] = '%s' AND [Tonnage Remaining] >= %s AND [State] = '%s' and [Sender] = '%s'
            """

            cursor = conn.cursor()
            cursor.execute(query, (
                auth_site, nepm, tonnage, state, sender
            ))
        elif direction == "Outgoing":
            query = """
                SELECT [ID], [Auth Site], [Auth Number], [Start Date], [Exp Date], [State],
                [Sender], [NEPM], [Phys State], [Tonnage Initial],
                [Tonnage Remaining], [Generator], [Responsible]
                FROM [Register].[Outgoing]
                WHERE [Auth Site] = '%s' AND [NEPM] = '%s' AND [Tonnage Remaining] >= %s AND [State] = '%s' and [Receiver] = '%s'
            """

            cursor = conn.cursor()
            cursor.execute(query, (
                auth_site, nepm, tonnage, state, sender
            ))

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
