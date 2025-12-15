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

        comp_note = data.get("comp_note")
        id_num = data.get("id_num")
        direction = data.get("direction")

        # === pymssql connection (use env vars from Azure settings) ===
        conn = pymssql.connect(
            server=os.getenv("SQL_SERVER"),
            user=os.getenv("SQL_USERNAME"),
            password=os.getenv("SQL_PASSWORD"),
            database=os.getenv("SQL_DATABASE"),
            port=1433
        )

        query = """
            UPDATE [Register].[%s]
            SET [Completion] = '%s' WHERE [ID] = %s
        """

        cursor = conn.cursor()
        cursor.execute(query, (direction, comp_note, id_num))
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
