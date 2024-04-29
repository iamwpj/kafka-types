from kafka.admin import KafkaAdminClient, NewTopic, NewPartitions
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

    def create_partitions(self, topic_partitions: NewPartitions ) -> None:
        return self.admin.create_partitions(topic_partitions=topic_partitions)