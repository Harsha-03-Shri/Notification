from cassandra.cluster import Cluster
import os
import time
import logging
from uuid import UUID, uuid4

logging.basicConfig(level=logging.INFO)

class CassandraService:
    def __init__(self):
        self.session = None

    def connect(self):
        host = os.getenv("CASSANDRA_HOST", "cassandra")
        port = int(os.getenv("CASSANDRA_PORT", "9042"))
        keyspace = os.getenv("CASSANDRA_KEYSPACE", "notification")

        cluster = Cluster(contact_points=[host], port=port)

        for attempt in range(10):
            try:
                self.session = cluster.connect(keyspace)
                logging.info("Connected to Cassandra")
                return
            except Exception as e:
                logging.warning(f"Cassandra not ready, retrying ({attempt+1}/10): {e}")
                time.sleep(3)

        raise Exception("Failed to connect to Cassandra")

    def get_user(self, user_id: UUID):
        query = "SELECT id, name, email, device_token FROM users WHERE id=%s"
        row = self.session.execute(query, (user_id,)).one()

        if not row:
            return None

        return {
            "id": row.id,
            "name": row.name,
            "email": row.email,
            "device_token": row.device_token
        }

    def get_template(self, event_type: str):
        query = "SELECT template FROM notification_templates WHERE event_type=%s"
        row = self.session.execute(query, (event_type,)).one()
        return row.template if row else ""

    def push_user(self, user_info: 'User') -> bool:
        from cassandra.query import SimpleStatement
        try:
            query = """
                INSERT INTO users (id, name, email, device_token)
                VALUES (%s, %s, %s, %s)
            """

            user_id = uuid4()  
        
            self.session.execute(
                query,
                (
                    user_id,
                    user_info.name,
                    user_info.email,
                    user_info.device_token
                )
            )
            logging.info(f"Inserted user {user_id} successfully")
            return True

        except Exception as e:
            logging.error(f"Failed to insert user {user_info}: {e}")
            return False

