from fastapi import FastAPI, HTTPException
from services.DB import CassandraService
from services.Kafka import AsyncKafkaProducerService
import asyncio 
from pydantic import BaseModel,EmailStr
from typing import Optional
from uuid import UUID
import logging

app = FastAPI()

db = CassandraService()
kafka = AsyncKafkaProducerService()


class User(BaseModel):
    name: str
    email: str
    device_token: Optional[str] = None

@app.on_event("startup")
async def startup():
    db.connect()
    await kafka.start()


@app.on_event("shutdown")
async def shutdown():
    await kafka.stop()

@app.post("/addUser")
async def add_user(user_info: User):
    success = db.push_user(user_info)

    if not success:
        raise HTTPException(
            status_code=500,
            detail="Failed to add user"
        )

    logging.info("Successfully pushed data into DB")
    return {"status": "user added successfully"}


@app.post("/notify/payment-success")
async def payment_success(user_id: UUID):
    user = db.get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    template = db.get_template("PAYMENT_SUCCESS")

    payload = {
        "email": user["email"],
        "device_token": user["device_token"],
        "message": template.format(name=user["name"]),
        "failure_type":[]
    }

    await send_function(payload)

    return {"status": "payment success notification sent"}


@app.post("/notify/course-completed")
async def course_completed(user_id: UUID):
    user = db.get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    template = db.get_template("COURSE_COMPLETED")

    payload = {
        "email": user["email"],
        "device_token": user["device_token"],
        "message": template.format(name=user["name"]),
        "failure_type":[]
    }

    await send_function(payload)


    return {"status": "course completion notification sent"}


@app.post("/notify/assignment-submitted")
async def assignment_submitted(user_id: UUID):
    user = db.get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    template = db.get_template("ASSIGNMENT_SUBMITTED")

    payload = {
        "email": user["email"],
        "device_token": user["device_token"],
        "message": template.format(name=user["name"]),
        "failure_type":[]
    }

    await send_function(payload)

    return {"status": "assignment submitted notification sent"}


async def send_function(payload: dict):
    try:
            await asyncio.gather(
        kafka.send_email(payload),
        kafka.send_apn(payload),
        kafka.send_fcm(payload)
    )
    except Exception as e:
        logging.error(f"Error while sending the notifications:{e}")