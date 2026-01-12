from producer.dlq import DlqProducer 
async def send_fcm(payload: dict,producer: DlqProducer):
    token = payload["device_token"]
    message = payload["message"]

    
    print(f"[FCM] Token={token} | Msg={message}")
