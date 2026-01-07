from cassandra.cluster import Cluster


class CassandraService:
    def __init__(self):
        cluster = Cluster(contact_points=["127.0.0.1"],port=9042)
        self.session = cluster.connect("notification")

    def get_user(self, user_id: str):
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
