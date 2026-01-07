import json
from aiokafka import AIOKafkaConsumer


async def create_consumer(topic: str, group_id: str):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers="localhost:29092",
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )
    await consumer.start()
    return consumer
