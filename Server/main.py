from fastapi import FastAPI, HTTPException
from services.DB import CassandraService
from services.Kafka import AsyncKafkaProducerService

app = FastAPI()

db = CassandraService()
kafka = AsyncKafkaProducerService()


@app.on_event("startup")
async def startup():
    await kafka.start()


@app.on_event("shutdown")
async def shutdown():
    await kafka.stop()


@app.post("/notify/payment-success")
async def payment_success(user_id: str):
    user = db.get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    template = db.get_template("PAYMENT_SUCCESS")

    payload = {
        "email": user["email"],
        "device_token": user["device_token"],
        "message": template.format(name=user["name"])
    }

    await kafka.send_email(payload)
    await kafka.send_apn(payload)
    await kafka.send_fcm(payload)

    return {"status": "payment success notification sent"}


@app.post("/notify/course-completed")
async def course_completed(user_id: str):
    user = db.get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    template = db.get_template("COURSE_COMPLETED")

    payload = {
        "email": user["email"],
        "device_token": user["device_token"],
        "message": template.format(name=user["name"])
    }

    await kafka.send_email(payload)
    await kafka.send_apn(payload)
    await kafka.send_fcm(payload)

    return {"status": "course completion notification sent"}


@app.post("/notify/assignment-submitted")
async def assignment_submitted(user_id: str):
    user = db.get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    template = db.get_template("ASSIGNMENT_SUBMITTED")

    payload = {
        "email": user["email"],
        "device_token": user["device_token"],
        "message": template.format(name=user["name"])
    }

    await kafka.send_email(payload)

    return {"status": "assignment submitted notification sent"}
