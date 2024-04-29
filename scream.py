from src.producer import Producer
import sys

# Create produce object
producer = Producer()

# Send a lot of logs.
k = 0
try:
    buff = ''
    while True:
        buff += sys.stdin.read(1)
        if buff.endswith('\n'):
            result = producer.send(
                topic="receiving", key=b"bltput", value=buff[:-1].encode()
            )
            buff = ''
            k = k+1

except KeyboardInterrupt:
    sys.stdout.flush()
    pass

print(k)