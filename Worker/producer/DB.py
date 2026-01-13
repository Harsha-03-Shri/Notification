from cassandra.cluster import Cluster
from datetime import datetime
import logging
import os
import time

logging.basicConfig(level=logging.INFO)

class CassandraService:
    _session = None  # class-level session, shared across instances

    def __init__(self):
        if not CassandraService._session:
            self.connect()
        self.session = CassandraService._session

    def connect(self):
        host = os.getenv("CASSANDRA_HOST", "cassandra")
        port = int(os.getenv("CASSANDRA_PORT", "9042"))
        keyspace = os.getenv("CASSANDRA_KEYSPACE", "notification")

        cluster = Cluster(contact_points=[host], port=port)

        for attempt in range(10):
            try:
                CassandraService._session = cluster.connect(keyspace)
                logging.info("Connected to Cassandra")
                return
            except Exception as e:
                logging.warning(f"Cassandra not ready, retrying ({attempt+1}/10): {e}")
                time.sleep(3)

        raise Exception("Failed to connect to Cassandra")

    async def put_dlq(self, payload: dict) -> bool:
        query = """
            INSERT INTO failure_dlq (failure_type, event_timestamp, error_log)
            VALUES (%s, %s, %s)
        """
        try:
            for failure in payload.get("failure_type", []):
                for failure_type, error in failure.items():
                    self.session.execute(
                        query,
                        (failure_type, datetime.utcnow(), str(error))
                    )
            return True
        except Exception as e:
            logging.error(f"Failed to push DLQ into Cassandra: {e}")
            return False
