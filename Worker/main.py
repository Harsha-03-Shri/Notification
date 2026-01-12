import asyncio
import signal
import logging

from kafka_consumer import create_consumer
from consumers.email import send_email
from consumers.fcm import send_fcm
from consumers.apn import send_apn
from consumers.dlq import push_to_db
from producer.dlq import DlqProducer

logging.basicConfig(level=logging.INFO)

dlq_producer = DlqProducer()




async def email_worker():
    consumer = await create_consumer("email", "email-workers")
    try:
        async for msg in consumer:
            await send_email(msg.value, dlq_producer)
    finally:
        await consumer.stop()


async def fcm_worker():
    consumer = await create_consumer("fcm", "fcm-workers")
    try:
        async for msg in consumer:
            await send_fcm(msg.value, dlq_producer)
    finally:
        await consumer.stop()


async def apn_worker():
    consumer = await create_consumer("apn", "apn-workers")
    try:
        async for msg in consumer:
            await send_apn(msg.value, dlq_producer)
    finally:
        await consumer.stop()


async def dlq_worker():
    consumer = await create_consumer("dlq", "dlq-workers")
    try:
        async for msg in consumer:
            await push_to_db(msg.value)
    finally:
        await consumer.stop()




async def startup():
    logging.info("Starting DLQ producer")
    await dlq_producer.start()


async def shutdown():
    logging.info("Shutting down DLQ producer")
    await dlq_producer.stop()




async def main():
    await startup()

    tasks = [
        asyncio.create_task(email_worker()),
        asyncio.create_task(fcm_worker()),
        asyncio.create_task(apn_worker()),
        asyncio.create_task(dlq_worker()),
    ]

    stop_event = asyncio.Event()

    def _signal_handler():
        stop_event.set()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, _signal_handler)
    loop.add_signal_handler(signal.SIGTERM, _signal_handler)

    await stop_event.wait()

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    await shutdown()


if __name__ == "__main__":
    asyncio.run(main())
