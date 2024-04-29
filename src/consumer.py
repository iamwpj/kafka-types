from kafka import KafkaConsumer
from src.bootstrap_servers import bootstrap_servers
from typing import List

class Consumer:
    def __init__(self,**kwargs):
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            **kwargs
        )
        self.consumer = consumer
        
    def topics(self) -> set:
        return self.consumer.topics()
    
    def poll(self,**kwargs) -> dict:
        result = self.consumer.poll(**kwargs)
        return result

    def subscribe(self,topics: List[str], **kwargs) -> set:
        self.consumer.subscribe(topics=topics,**kwargs)
        return self.consumer.subscription()
            
    def status(self) -> bool:
        result = self.consumer.bootstrap_connected()
        return result
    
    def close(self, **kwargs) -> None:
        return self.consumer.close(**kwargs)
    