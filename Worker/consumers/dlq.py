import logging
from producer.DB import CassandraService

logging.basicConfig(level=logging.INFO)

db = CassandraService()
async def push_to_db(payload: dict):
            try:
                await db.put_dlq(payload)
                logging.info("Pushed failure message into DB from DLQ consumer")
            except Exception as e:
                logging.error(
                    f"While pushing DLQ into DB: {e}",
                    exc_info=True
                )
