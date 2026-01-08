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

outlook_addr = str(os.getenv("OUTLOOK_FROM"))
outlook_pwsd = str(os.getenv("OUTLOOK_PASSWORD"))

DOMAIN_REGEX = r'(?<=@)[^@]+$'

def extract_domain(email: str) -> str:
    match = re.search(DOMAIN_REGEX, email)
    if not match:
        raise ValueError(f"Invalid email address: {email}")
    return match.group(0).lower()


def send_gmail(msg: EmailMessage):
    msg["From"] = gmail_addr
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(gmail_addr,gmail_pwsd)
        server.send_message(msg)


def send_outlook(msg: EmailMessage):
    msg["From"] = outlook_addr
    with smtplib.SMTP("smtp.office365.com", 587) as server:
        server.starttls()
        server.login(outlook_addr, outlook_pwsd)
        server.send_message(msg)


def send_default(msg: EmailMessage):
    with smtplib.SMTP("smtp.yourdomain.com", 587) as server:
        server.starttls()
        server.login("user@yourdomain.com", "PASSWORD")
        server.send_message(msg)


DOMAIN_DISPATCHER: dict[str, Callable[[EmailMessage], None]] = {
    "gmail.com": send_gmail,
    "outlook.com": send_outlook,
    "hotmail.com": send_outlook
}


def send_mail(
    to_email: str,
    body: str,
    subject: Optional[str]="Subject",
):
    domain = extract_domain(to_email)

    msg = EmailMessage()
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    sender = DOMAIN_DISPATCHER.get(domain, send_default)
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
