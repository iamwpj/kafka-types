from src.producer import Producer
from src.consumer import Consumer
from src.grokker import Grokker
from src.admin import Admin, NewTopic
from src.schemas import Schema
import config.config as c
import time
import os


# Create a new topic
def create_topic() -> bool:
    """Create a destination topic -- only if not
    already exists.

    Returns:
        bool: Returns true if topics exist or are created.
    """

    topics = consumer.topics()
    create = [
        True if topic in [c.dest_topic, c.src_topic] else False for topic in topics
    ]

    if not create:
        admin = Admin()
        topics = [NewTopic(name=c.dest_topic, num_partitions=c.partition_count, replication_factor=1)]

        result = admin.create_topics(topics=[c.dest_topic, c.src_topic])

        if result.error_code == 0:
            return True
        else:
            print(result.topic_errors)
            return False

    return True


# Catch new messages
def ouptut() -> str:
    """This creates a consumer and watches for message on an assigned topic (c.src_topic)

    Returns:
        str: Returns string formatted message (decoded from bytecode).
    """

    counter = 0

    try:
        consumer.subscribe([c.src_topic])
        while True:
            # Build our env
            grok = Grokker()
            schema = Schema(c.common_key)

            msgbatch = consumer.poll(timeout_ms=1)
            if msgbatch:
                for msgobj in msgbatch:
                    counter += len(msgbatch[msgobj])
                    print(f"message batch count: {counter}")
                    runtime = time.time()
                    [
                        fixer(msg.value.decode("utf-8"), grok, schema)
                        for msg in msgbatch[msgobj]
                    ]
                    print(f"Batch time:\t\t{time.time()-runtime}")
    except KeyboardInterrupt:
        consumer.close()
        pass


# Fixer
def fixer(msg: str, grok, schema):
    """This receives unformatted logs from the source Kafka topic
    and passes them to the groker -> applies schema -> and then
    submits them to the destination Kafka topic.

    Args:
        msg (str): The UTF8 text for encoding in Grok and Avro.
        grokker: This utility is pre-loaded to save time.
        schemer: This utility is pre-loaded to save time.
    """
    # runtime = time.time()
    # grok
    parsed = grok.default(msg)
    key, updated = grok.auto_schema_gen(data=parsed)
    # print(f"Grok time:\t\t{time.time()-runtime}")

    # schema apply
    if not key == c.common_key:
        schema = Schema(key)

    bytes_data = schema.apply([updated])
    # print(f"Schema time:\t\t{time.time()-runtime}")

    # Submit to desination topic
    producer.send(topic=c.dest_topic, value=bytes_data, key=key.encode())
    # print(f"Producer time:\t\t{time.time()-runtime}")


if __name__ == "__main__":
    # Build necessary objects
    consumer = Consumer(
        max_poll_records=10000,
        auto_offset_reset="earliest",
        client_id=f"injection_{os.getpid()}",
        group_id="injection_group",
    )
    producer = Producer()

    # Run functions
    create_topic()
    ouptut()
