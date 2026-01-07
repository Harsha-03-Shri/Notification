async def send_fcm(payload: dict):
    token = payload["device_token"]
    message = payload["message"]

    
    print(f"[FCM] Token={token} | Msg={message}")
