import os
import json
from aiokafka import AIOKafkaProducer


class AsyncKafkaProducerService:
    def __init__(self):
        self.bootstrap_servers = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "kafka1:9092"
        )
        self.producer = None

    async def start(self):
        if self.producer:
            return

        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await self.producer.start()

    async def stop(self):
        if self.producer:
            await self.producer.stop()
            self.producer = None

    async def send_email(self, message: dict):
        await self._send("email", message)

    async def send_apn(self, message: dict):
        await self._send("apn", message)

    async def send_fcm(self, message: dict):
        await self._send("fcm", message)

    async def send_dlq(self, message: dict):
        await self._send("dlq", message)

    async def _send(self, topic: str, message: dict):
        if not self.producer:
            raise RuntimeError("Kafka producer not started")

        await self.producer.send_and_wait(topic, message)
