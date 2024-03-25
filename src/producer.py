from kafka import KafkaProducer
from kafka.producer.future import FutureRecordMetadata
from src.bootstrap_servers import bootstrap_servers


class Producer:
    def __init__(self):
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        self.producer = producer

    def send(self, **kwargs) -> FutureRecordMetadata:
        result = self.producer.send(**kwargs)
        return result

    def status(self) -> bool:
        return self.producer.bootstrap_connected()
