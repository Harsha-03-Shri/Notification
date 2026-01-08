import re
import smtplib
from email.message import EmailMessage
from dotenv import load_dotenv
from typing import Callable,Optional
import logging 
import os 


logging.basicConfig(level=logging.INFO)
load_dotenv()

gmail_addr = str(os.getenv("GMAIL_FROM"))
gmail_pwsd = str(os.getenv("GMAIL_PASSWORD"))

def sender(msg: EmailMessage):
    msg["From"] = gmail_addr
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(gmail_addr,gmail_pwsd)
        server.send_message(msg)

def send_mail(
    to_email: str,
    body: str,
    subject: Optional[str]="Subject",
):

    msg = EmailMessage()
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    sender(msg)

async def send_email(payload: dict):
    email = payload["email"]
    message = payload["message"]
    try:
        send_mail(email,message)
        logging.info(f"Mail sent successfully to {email}")
    except Exception as e:

        logging.error(f"Failed sending the mail to {email} -> {e}")

    # print(f"[EMAIL] To={email} | Msg={message}")
