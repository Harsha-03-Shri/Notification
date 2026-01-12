from cassandra.cluster import Cluster
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)

class CassandraService:
    def __init__(self):
        cluster = Cluster(contact_points=["127.0.0.1"],port=9042)
        self.session = cluster.connect("notification")

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

        except Exception:
            return False