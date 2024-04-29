from src.consumer import Consumer
import config.config as c

consumer = Consumer(
        group_id="remote-con-1", auto_offset_reset="earliest"
    )

try:
    consumer.subscribe([c.src_topic])
    while True:
        msgbatch = consumer.poll(timeout_ms=1)
        if msgbatch:
            for msgobj in msgbatch:
                [ print (msg.value.decode('utf-8')) for msg in msgbatch[msgobj] ]
    
except KeyboardInterrupt:
    consumer.close()
    pass

# {TopicPartition(topic='test', partition=0): [ConsumerRecord(topic='test', partition=0, offset=22, timestamp=1714191299410, timestamp_type=0, key=None, value=b'Finally a real live test?', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=25, serialized_header_size=-1)]}