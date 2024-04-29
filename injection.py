from src.producer import Producer
from src.consumer import Consumer
from src.grokker import Grokker
from src.admin import Admin, NewTopic
from src.schemas import Schema
import config.config as c


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
        topics = [NewTopic(name=c.dest_topic, num_partitions=1, replication_factor=1)]

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
                    counter += len(msgobj)
                    print(f"message batch count: {counter}")
                    [
                        fixer(msg.value.decode("utf-8"), grok, schema)
                        for msg in msgbatch[msgobj]
                    ]
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

    # grok
    parsed = grok.default(msg)
    key, updated = grok.auto_schema_gen(data=parsed)

    # schema apply
    if not key == c.common_key:
        schema = Schema(key)

    bytes_data = schema.apply([updated])

    # Submit to desination topic
    producer.send(topic=c.dest_topic, value=bytes_data, key=key.encode())


if __name__ == "__main__":
    consumer = Consumer(
        max_poll_records=100,
        auto_offset_reset="earliest",
    )
    producer = Producer()

    create_topic()
    ouptut()
