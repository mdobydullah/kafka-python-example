import json
from time import sleep

from kafka import KafkaProducer
from json import dumps
from faker import Faker

try:
    print('Kafka Producer!')

    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        # value_serializer=lambda m: json.dumps(m).encode('ascii'),
        # value_serializer=lambda m: dumps(m).encode('utf-8'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # print(producer.config)
    connected = producer.bootstrap_connected()
    print(f"Connected: {connected}")
    print("\n")

    print("Sending data:")
    for i in range(50):
        i += 1
        print(f"Data {i}")

        # print(producer.send('topic', b'hello').get(timeout=1))
        # message = producer.send("topic", {"hello": "producer"}).get(timeout=1)
        # message = producer.send("topic", value={"hello": "producer"}).get(timeout=1)

        fake = Faker()
        message = {
            'name': fake.name(),
            'address': fake.address()
        }
        sent = producer.send("topic", value=message).get(timeout=1)

        # producer.flush()
        # print(sent.topic)
        # print(sent.partition)
        # print(sent.offset)
        sleep(1)

except Exception as e:
    print(e)
    # Logs the error appropriately.
    pass
