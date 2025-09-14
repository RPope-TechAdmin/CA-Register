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
        start_date = data.get("start_date")
        exp_date = data.get("exp_date")
        state = data.get("state")
        sender = data.get("customer")
        sender_license = data.get("cust_lic")
        destination = data.get("destination")
        transporter = data.get("transporter")
        trans_lic = data.get("trans_lic")
        nepm = data.get("nepm")
        description = data.get("description")
        phys_state = data.get("phys_state")
        tonnage_initial = data.get("tonnage")
        tonnage_remaining = data.get("tonnage")
        generator = data.get("generator")
        responsible = data.get("applicant")

        # === pymssql connection (use env vars from Azure settings) ===
        conn = pymssql.connect(
            server=os.getenv("SQL_SERVER"),
            user=os.getenv("SQL_USERNAME"),
            password=os.getenv("SQL_PASSWORD"),
            database=os.getenv("SQL_DATABASE"),
            port=1433
        )

        query = """
            INSERT INTO [Register].[Incoming] 
            ([Auth Site], [Auth Number], [Start Date], [Exp Date], [State],
             [Sender], [License], [Detination], [Transporter], [Transporter License],
             [NEPM], [Description], [Phys State], [Tonnage Initial],
             [Tonnage Remaining], [Generator], [Responsible])
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor = conn.cursor()
        cursor.execute(query, (
            auth_site, auth_number, start_date, exp_date, state,
            sender, sender_license, destination, transporter, trans_lic,
            nepm, description, phys_state, tonnage_initial,
            tonnage_remaining, generator, responsible
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
