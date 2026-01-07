import asyncio
from kafka_consumer import create_consumer
from consumers.email import send_email
from consumers.fcm import send_fcm
from consumers.apn import send_apn


async def email_worker():
    consumer = await create_consumer("email", "email-workers")
    try:
        async for msg in consumer:
            await send_email(msg.value)
    finally:
        await consumer.stop()


async def fcm_worker():
    consumer = await create_consumer("fcm", "fcm-workers")
    try:
        async for msg in consumer:
            await send_fcm(msg.value)
    finally:
        await consumer.stop()


async def apn_worker():
    consumer = await create_consumer("apn", "apn-workers")
    try:
        async for msg in consumer:
            await send_apn(msg.value)
    finally:
        await consumer.stop()


async def main():
    await asyncio.gather(
        email_worker(),
        fcm_worker(),
        apn_worker()
    )


if __name__ == "__main__":
    asyncio.run(main())
