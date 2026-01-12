# consumers/email.py

import aiosmtplib
from email.message import EmailMessage
from dotenv import load_dotenv
from typing import Optional
import logging
import os

from producer.dlq import DlqProducer
from email_validator import validate_email, EmailNotValidError

logging.basicConfig(level=logging.INFO)
load_dotenv()

gmail_addr = os.getenv("GMAIL_FROM")
gmail_pwsd = os.getenv("GMAIL_PASSWORD")


ALLOWED_DOMAINS = {"gmail.com", "yahoo.com", "outlook.com"}

def validate_email_address(email: str) -> bool:
    domain = email.split("@")[-1].lower()
    if domain not in ALLOWED_DOMAINS:
        logging.warning(f"Disallowed domain: {domain}")
        return False
    return True


async def sender(msg: EmailMessage):

    msg["From"] = gmail_addr

    await aiosmtplib.send(
        msg,
        hostname="smtp.gmail.com",
        port=465,
        username=gmail_addr,
        password=gmail_pwsd,
        use_tls=True,
    )


async def send_mail(to_email: str, body: str, subject: Optional[str] = "Subject"):

    msg = EmailMessage()
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    await sender(msg)


async def send_email(payload: dict, producer: DlqProducer):

    email = payload.get("email")
    message = payload.get("message")
    subject = payload.get("subject", "Notification")

  
    if not validate_email_address(email):
        payload.setdefault("failure_type", []).append({"email": "Invalid email"})
        try:
            await producer.send_dlq(payload)
            logging.info(f"Invalid email -> pushed to DLQ: {email}")
        except Exception:
            logging.error("DLQ push failed for invalid email", exc_info=True)
        return

  
    try:
        await send_mail(email, message, subject)
        logging.info(f"Mail accepted by SMTP for delivery: {email}")

    except Exception as e:
       
        payload.setdefault("failure_type", []).append({"email": str(e)})
        try:
            await producer.send_dlq(payload)
            logging.info(f"Failed mail -> pushed to DLQ: {email}")
        except Exception:
            logging.error("DLQ push failed", exc_info=True)
