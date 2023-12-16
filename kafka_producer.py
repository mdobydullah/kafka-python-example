from kafka import KafkaProducer
from json import dumps

try:
    print('Welcome to Kafka Producer!')

    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        # value_serializer=lambda m: json.dumps(m).encode('ascii'),
        value_serializer=lambda m: dumps(m).encode('utf-8'),
    )

    # print(producer.config)
    print(producer.bootstrap_connected())

    for _ in range(1):
        print("Sending messages...")

        # print(producer.send('topic', b'another_message').get(timeout=1))
        # print(producer.send("topic", value={"hello": "producer"}).get(timeout=1))

        data = producer.send("topic", value={"hello": "producer"}).get(timeout=1)

        # producer.flush()
        print(data.topic)
        print(data.partition)
        print(data.offset)

except Exception as e:
    print(e)
    # Logs the error appropriately.
    pass
