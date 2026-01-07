import json
from aiokafka import AIOKafkaProducer


class AsyncKafkaProducerService:
    def __init__(self):
        self.bootstrap_servers = "localhost:29092"
        self.producer = None

    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        await self.producer.start()

    async def stop(self):
        if self.producer:
            await self.producer.stop()

    async def send_email(self, message: dict):
        await self.producer.send_and_wait("email", message)

    async def send_apn(self, message: dict):
        await self.producer.send_and_wait("apn", message)

    async def send_fcm(self, message: dict):
        await self.producer.send_and_wait("fcm", message)
