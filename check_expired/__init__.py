import logging
import azure.functions as func
import pymssql
import os
from datetime import datetime, timedelta
import smtplib
from email.message import EmailMessage


def send_email(recipient: str, subject: str, body: str) -> None:
    """Send email using Gmail SMTP."""
    sender = os.getenv("FEEDBACK_EMAIL")
    eml_pass = os.getenv("FEEDBACK_PASS")

    if not sender or not eml_pass:
        raise EnvironmentError("Missing FEEDBACK_EMAIL or FEEDBACK_PASS environment variables")

    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
            smtp.starttls()
            smtp.login(sender, eml_pass)
            smtp.send_message(msg)
        logging.info(f"✅ Email sent to {recipient}")
    except Exception as e:
        logging.exception(f"❌ Failed to send email to {recipient}: {e}")


def main(mytimer: func.TimerRequest) -> None:
    logging.info("⏰ Checking for expiring authorisations...")

    # === SQL CONNECTION VARIABLES FROM APP SETTINGS ===
    server = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DATABASE")
    username = os.getenv("SQL_USERNAME")
    password = os.getenv("SQL_PASSWORD")

    if not all([server, database, username, password]):
        logging.error("❌ Missing one or more SQL connection environment variables.")
        return

    today = datetime.now()
    cutoff = today + timedelta(days=45)

    query = """
        SELECT [Auth Number], [Exp Date], [Responsible]
        FROM [Register].[Incoming]
        WHERE [Exp Date] <= %s AND [Exp Date] >= %s
    """

    try:
        with pymssql.connect(server, username, password, database) as conn:
            cursor = conn.cursor(as_dict=True)
            cursor.execute(query, (cutoff, today))
            rows = cursor.fetchall()

            if not rows:
                logging.info("✅ No authorisations expiring within 5 days.")
                return

            # Group by Responsible person
            grouped = {}
            for row in rows:
                resp = row.get("Responsible") or "Unknown"
                grouped.setdefault(resp, []).append(row)

            # Send email for each responsible user
            for responsible, items in grouped.items():
                recipient_email = "rpope@purenv.au"

                body_lines = [
                    f"Hello,",
                    "",
                    "The following incoming CA's are expiring soon:",
                    ""
                ]
                for item in items:
                    exp_date = item.get("Exp Date")
                    if isinstance(exp_date, datetime):
                        exp_date = exp_date.strftime("%Y-%m-%d")
                    body_lines.append(f"- Auth {item['Auth Number']} (expires {exp_date}) - Responsible: {responsible}")

                body_lines.append("")
                body_lines.append("Please review and renew as necessary.")
                body_lines.append("Thanks,")
                body_lines.append("The CA Register Check Bot")
                body = "\n".join(body_lines)

                send_email(recipient_email, "⚠️ Upcoming Authorisation Expiry", body)

    except Exception as e:
        logging.exception(f"❌ Database query or email failed: {e}")

