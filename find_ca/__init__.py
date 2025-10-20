import logging
import azure.functions as func
import pymssql
import os
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing find_ca request.")

    try:
        # Parse JSON body from frontend
        data = req.get_json()

        auth_site = data.get("auth_site")
        state = data.get("state")
        customer = data.get("customer")
        nepm = data.get("nepm")
        tonnage = float(data.get("tonnage", 0)) # Convert tonnage to a float
        direction = data.get("direction")
        
        
        # === pymssql connection (use env vars from Azure settings) ===
        conn = pymssql.connect(
            server=os.getenv("SQL_SERVER"),
            user=os.getenv("SQL_USERNAME"),
            password=os.getenv("SQL_PASSWORD"),
            database=os.getenv("SQL_DATABASE"),
            port=1433
        )

        params = []
        where_clauses = []

        if auth_site:
            where_clauses.append("[Auth Site] = %s")
            params.append(auth_site)
        if nepm:
            where_clauses.append("[NEPM] = %s")
            params.append(nepm)
        if state:
            where_clauses.append("[State] = %s")
            params.append(state)
        
        where_clauses.append("[Tonnage Remaining] >= %s")
        params.append(tonnage)

        if direction=="Incoming":
            base_query = "SELECT [ID], [Auth Site], [Auth Number], [Start Date], [Exp Date], [State], [Sender], [NEPM], [Phys State], [Tonnage Initial], [Tonnage Remaining], [Generator], [Responsible] FROM [Register].[Incoming]"
            if customer:
                where_clauses.append("[Sender] = %s")
                params.append(customer)
        elif direction == "Outgoing":
            base_query = "SELECT [ID], [Auth Site], [Auth Number], [Start Date], [Exp Date], [State], [Receiver], [NEPM], [Phys State], [Tonnage Initial], [Tonnage Remaining], [Generator], [Responsible] FROM [Register].[Outgoing]"
            if customer:
                where_clauses.append("[Receiver] = %s")
                params.append(customer)
        else:
            return func.HttpResponse(body=json.dumps({"error": "Invalid direction specified"}), mimetype="application/json", status_code=400)

        query = f"{base_query} WHERE {' AND '.join(where_clauses)}"

        cursor = conn.cursor(as_dict=True)
        cursor.execute(query, tuple(params))
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
