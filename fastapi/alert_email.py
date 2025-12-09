# alert_email.py — container-friendly, minimal logging, uses env vars
import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
from datetime import datetime

logger = logging.getLogger("alert_email")
logging.basicConfig(level=logging.INFO)

# Read credentials from environment (preferred in Docker)
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
ALERT_RECEIVER = os.getenv("ALERT_RECEIVER")  # supports multiple recipients
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))

def send_alert(transaction_id: str, probability: float, amount: float = None) -> bool:
    """
    Send HTML email alert. Returns True on success, False on failure.
    Uses environment variables; no dotenv inside containers.
    Minimal changes only: multi-recipient, year auto-update, SSL vs TLS handling.
    """
    if not (EMAIL_USER and EMAIL_PASS and ALERT_RECEIVER):
        logger.warning("Email not configured (EMAIL_USER/EMAIL_PASS/ALERT_RECEIVER missing). Skipping alert.")
        return False

    # allow comma-separated recipients
    recipients = [r.strip() for r in ALERT_RECEIVER.split(",") if r.strip()]
    if not recipients:
        logger.warning("ALERT_RECEIVER parsed into empty list — skipping alert.")
        return False

    subject = f"⚠️ Fraudulent Transaction Detected: {transaction_id}"
    current_year = datetime.utcnow().year

    # FULL HTML preserved exactly
    body = f"""
    <html>
        <body style="font-family: Arial, sans-serif; background-color: #f7f9fc; padding: 20px;">
            <div style="max-width: 600px; margin: auto; background-color: #ffffff; 
                        border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); padding: 20px;">
                <h2 style="color: #d9534f;">⚠️ Fraud Alert Notification</h2>
                <p>Dear User,</p>
                <p>Our fraud detection system has flagged a suspicious transaction with the following details:</p>

                <table style="width: 100%; border-collapse: collapse; margin-top: 15px;">
                    <tr>
                        <td style="padding: 8px; border-bottom: 1px solid #eee;"><strong>Transaction ID:</strong></td>
                        <td style="padding: 8px; border-bottom: 1px solid #eee;">{transaction_id}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px; border-bottom: 1px solid #eee;"><strong>Fraud Probability:</strong></td>
                        <td style="padding: 8px; border-bottom: 1px solid #eee;">{probability:.4f}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px;"><strong>Amount:</strong></td>
                        <td style="padding: 8px;">{amount if amount is not None else 'N/A'}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px;"><strong>Status:</strong></td>
                        <td style="padding: 8px;"><span style="color: #d9534f; font-weight: bold;">Flagged as FRAUD</span></td>
                    </tr>
                </table>

                <p style="margin-top: 20px;">Please review this transaction immediately via your account dashboard or contact our fraud prevention team.</p>

                <div style="margin-top: 25px; font-size: 12px; color: #999;">
                    <hr>
                    <p>This is an automated alert from your Fraud Detection System. Please do not reply to this email.</p>
                    <p>© {current_year} Fraud Detection System</p>
                </div>
            </div>
        </body>
    </html>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = EMAIL_USER
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(body, "html"))

    try:
        # Use SMTP_SSL if port is 465 (pure SSL)
        if SMTP_PORT == 465:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=20) as server:
                server.login(EMAIL_USER, EMAIL_PASS)
                server.sendmail(EMAIL_USER, recipients, msg.as_string())
        else:
            # Use TLS for ports such as 587
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as server:
                server.ehlo()
                server.starttls()
                server.ehlo()
                server.login(EMAIL_USER, EMAIL_PASS)
                server.sendmail(EMAIL_USER, recipients, msg.as_string())

        logger.info("📧 Fraud alert email sent to %s for %s", recipients, transaction_id)
        return True

    except smtplib.SMTPAuthenticationError as e:
        logger.exception("⚠️ SMTP authentication failed for %s: %s", transaction_id, e)
        return False
    except Exception as e:
        logger.exception("⚠️ Email sending failed for %s: %s", transaction_id, e)
        return False