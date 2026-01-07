async def send_apn(payload: dict):
    token = payload["device_token"]
    message = payload["message"]

   
    print(f"[APN] Token={token} | Msg={message}")
