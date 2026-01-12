from producer.dlq import DlqProducer 
async def send_apn(payload: dict,producer: DlqProducer):
    token = payload["device_token"]
    message = payload["message"]

   
    print(f"[APN] Token={token} | Msg={message}")
