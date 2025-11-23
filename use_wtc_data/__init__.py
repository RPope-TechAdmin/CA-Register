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
        cust_name=data.get("cust_name")
        ship_date = data.get("ship_date")
        use_date = data.get("use_date")
        direction = data.get("direction")   # "Incoming" or "Outgoing"
        wtcqld = data.get("wtcqld")
        wtcext = data.get("wtcext")
        nepm = data.get("nepm")
        tonnage = data.get("ton")
        responsible = data.get("user")

        # === Connect with pymssql ===
        conn = pymssql.connect(
            server=os.getenv("SQL_SERVER"),
            user=os.getenv("SQL_USERNAME"),
            password=os.getenv("SQL_PASSWORD"),
            database=os.getenv("SQL_DATABASE"),
            port=1433
        )
        cursor = conn.cursor()
        
        # ======================================================
        # 1️⃣ CHECK IF AUTH NUMBER EXISTS IN Incoming/Outgoing
        # ======================================================

        query_check = f"""
            SELECT COUNT(*) 
            FROM [Register].[{direction}]
            WHERE [Auth Number] = %s AND [NEPM] = %s
        """

        cursor.execute(query_check, (auth_number, nepm))
        (count,) = cursor.fetchone()

        if count == 0:
            conn.close()
            return func.HttpResponse(
                body=json.dumps({
                    "error": f"Auth Number '{auth_number}' does not exist in {direction} register."
                }),
                mimetype="application/json",
                status_code=400
            )

        # ======================================================
        # 2️⃣ INSERT INTO WTC TABLE (safe because record exists)
        # ======================================================

        query_insert = f"""
            INSERT INTO [Register].[WTC] 
            ([Auth Site], [Auth Number], [Shipping Date], [Use Date], [Incoming/Outgoing],
             [WTC QLD], [WTC Ext], [NEPM], [Tonnage], [Authorised By], [Customer])
             VALUES ('{auth_site}', '{auth_number}', '{ship_date}', '{use_date}', '{direction}', '{wtcqld}', '{wtcext}', '{nepm}', {tonnage}, '{responsible}', '{cust_name}')
        """
        logging.info(f"Insert Query: {query_insert}")
        cursor.execute(query_insert)

        if auth_site and tonnage:
        # ======================================================
        # 3️⃣ UPDATE TONNAGE REMAINING
        # ======================================================
            query_update = f"""
                UPDATE [Register].[{direction}]
                SET [Tonnage Remaining] = [Tonnage Remaining] - {tonnage}
                WHERE [Auth Number] = '{auth_number}' AND [NEPM] = '{nepm}'
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
        logging.error(f"Insert failed, please ensure all information is formatted correctly. Error: {e}")
        return func.HttpResponse(
            body=json.dumps({"error": {e}}),
            mimetype="application/json",
            status_code=500
        )
