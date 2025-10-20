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
        state = data.get("state") # This is not used in the query
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
            # Note: 'state' and 'sender' from payload are not used here.
            query = """
                SELECT [ID], [Auth Site], [Auth Number], [Start Date], [Exp Date], [State],
                [Sender], [NEPM], [Phys State], [Tonnage Initial],
                [Tonnage Remaining], [Generator], [Responsible]
                FROM [Register].[Incoming]
                WHERE [Auth Site] = %s AND [NEPM] = %s AND [Tonnage Remaining] >= %s
            """

            cursor = conn.cursor(as_dict=True)
            cursor.execute(query, (auth_site, nepm, tonnage))

        elif direction == "Outgoing":
            # Note: 'state' and 'sender' (customer) from payload are not used here.
            query = """
                SELECT [ID], [Auth Site], [Auth Number], [Start Date], [Exp Date], [State],
                [Receiver], [NEPM], [Phys State], [Tonnage Initial],
                [Tonnage Remaining], [Generator], [Responsible]
                FROM [Register].[Outgoing]
                WHERE [Auth Site] = %s AND [NEPM] = %s AND [Tonnage Remaining] >= %s
            """

            cursor = conn.cursor(as_dict=True)
            cursor.execute(query, (auth_site, nepm, tonnage))

        rows = cursor.fetchall()

        conn.commit()
        conn.close()

        if not rows:
            return func.HttpResponse(
                body=json.dumps({"message": "No matching CAs found."}),
                mimetype="application/json",
                status_code=404
            )

        return func.HttpResponse(
            body=json.dumps({"results": rows}, default=str),
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
