import json
import logging
from aiokafka import AIOKafkaProducer
from cassandra.cluster import Cluster
from .DB import CassandraService

logging.basicConfig(level=logging.INFO)


class DlqProducer:
    def __init__(self):
        self.bootstrap_servers = "kafka1:9092"
        self.producer = None
        self.db = CassandraService()

    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await self.producer.start()

    async def stop(self):
        if self.producer:
            await self.producer.stop()

    async def send_dlq(self, payload: dict):
        try:
            await self.producer.send_and_wait("dlq",payload)
            logging.info(f"Pushed into DLQ from producer")
        except Exception as e:
            logging.error(f"While pushing DLQ into DB: {e}", exc_info=True)

