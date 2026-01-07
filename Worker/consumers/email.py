async def send_email(payload: dict):
    email = payload["email"]
    message = payload["message"]

    
    print(f"[EMAIL] To={email} | Msg={message}")
