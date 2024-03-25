from kafka import KafkaConsumer
from src.bootstrap_servers import bootstrap_servers
from typing import List

class Consumer:
    def __init__(self,topics: List[str] | str,**kwargs):
        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            **kwargs
        )
        self.consumer = consumer
    
    def poll(self,**kwargs) -> dict:
        result = self.consumer.poll(**kwargs)
        return result

    def subscribe(self,topics: List[str], **kwargs) -> None:
        self.consumer.subscribe(topics=topics,**kwargs)
    
    def status(self) -> bool:
        return self.consumer.bootstrap_connected()