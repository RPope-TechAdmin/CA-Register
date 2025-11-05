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

    logging.debug(f"Preparing to send email to: {recipient}")
    logging.debug(f"Email sender: {sender}, Password present: {bool(eml_pass)}")

    if not sender or not eml_pass:
        raise EnvironmentError("Missing FEEDBACK_EMAIL or FEEDBACK_PASS environment variables")

    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
            smtp.set_debuglevel(1)  # verbose SMTP logs
            smtp.starttls()
            smtp.login(sender, eml_pass)
            smtp.send_message(msg)
        logging.info(f"✅ Email successfully sent to {recipient}")
    except smtplib.SMTPAuthenticationError:
        logging.exception("❌ Authentication failed: Check Gmail App Password or FEEDBACK_PASS env var.")
        raise
    except smtplib.SMTPConnectError:
        logging.exception("❌ Connection to Gmail SMTP failed — check network/firewall.")
        raise
    except Exception as e:
        logging.exception(f"❌ Unexpected SMTP error: {e}")
        raise


def main(mytimer: func.TimerRequest) -> None:
    logging.info("⏰ Starting CA Expiry Checker Timer Function")

    # === SQL CONNECTION VARIABLES FROM APP SETTINGS ===
    server = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DATABASE")
    username = os.getenv("SQL_USERNAME")
    password = os.getenv("SQL_PASSWORD")

    logging.debug(f"SQL_SERVER: {server}")
    logging.debug(f"SQL_DATABASE: {database}")
    logging.debug(f"SQL_USERNAME: {username}")
    logging.debug(f"SQL_PASSWORD present: {bool(password)}")

    if not all([server, database, username, password]):
        logging.error("❌ Missing one or more SQL connection environment variables.")
        return

    today = datetime.now()
    cutoff = today + timedelta(days=45)
    logging.info(f"Date range: {today.date()} → {cutoff.date()}")

    query = """
        SELECT [Auth Number], [Exp Date], [Responsible]
        FROM [Register].[Outgoing]
        WHERE [Exp Date] <= %s AND [Exp Date] >= %s
    """

    try:
        logging.info("🔌 Connecting to SQL Server...")
        with pymssql.connect(server, username, password, database) as conn:
            logging.info("✅ SQL connection established successfully")

            cursor = conn.cursor(as_dict=True)
            logging.debug("Executing SQL query...")
            cursor.execute(query, (cutoff, today))
            rows = cursor.fetchall()

            logging.info(f"📦 Query executed. Rows fetched: {len(rows)}")

            if not rows:
                logging.info("✅ No authorisations expiring within 45 days.")
                return

            # Log sample row for debugging
            logging.debug(f"First row sample: {rows[0] if rows else 'No data'}")

            today=datetime.now()

            # Build one consolidated email
            recipient_email = "rpope@purenv.au; mgrave@purenv.au"
            body_lines = [
                f"Hello,",
                "",
                "The following outgoing CA's are expiring soon (within the next 45 days):",
                ""
            ]

            for item in rows:
                exp_date = item.get("Exp Date")
                auth_no = item.get("Auth Number", "Unknown")
                responsible = item.get("Responsible", "Unknown")
                if not responsible:
                    responsible= "Unknown"

                if isinstance(exp_date, datetime):
                    days_remaining = (exp_date.date() - today.date()).days
                    exp_str = exp_date.strftime("%Y-%m-%d")
                else:
                    # handle if date comes back as just a date
                    exp_str = str(exp_date)
                    try:
                        days_remaining = (datetime.strptime(exp_str, "%Y-%m-%d").date() - today.date()).days
                    except Exception:
                        days_remaining = "?"

                body_lines.append(
                    f"- Auth {auth_no} (expires {exp_str} — in {days_remaining} days ({exp_date})) - Responsible: {responsible}"
                )


            body_lines.extend([
                "",
                "Please review and renew as necessary.",
                "",
                "Thanks,",
                "The CA Register Check Bot"
            ])
            body = "\n".join(body_lines)

            try:
                send_email(recipient_email, "⚠️ Upcoming outgoing CA Expiries", body)
            except Exception as e:
                logging.error("❌ Failed to send consolidated expiry email.")
                logging.exception(e)

    except pymssql.InterfaceError:
        logging.exception("❌ Database interface error: check SQL connection string or credentials.")
    except pymssql.OperationalError:
        logging.exception("❌ Operational SQL error: possible network/timeout issue.")
    except Exception as e:
        logging.exception(f"❌ Unexpected error during DB operation: {e}")

    logging.info("✅ Function execution complete.")
