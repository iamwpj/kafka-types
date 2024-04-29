from kafka.admin import KafkaAdminClient, NewTopic
from src.bootstrap_servers import bootstrap_servers
from typing import List


class Admin:
    def __init__(self, **kwargs):
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            **kwargs,
        )
        self.admin = admin

    def create_topics(self, topics=List[NewTopic], **kwargs) -> List[tuple]:
        result = self.admin.create_topics(new_topics=topics, **kwargs)
        return result
