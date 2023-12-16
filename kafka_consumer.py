from kafka import KafkaConsumer
import json

try:
    print('Welcome to Kafka consumer')

    consumer = KafkaConsumer(
        'topic',
        bootstrap_servers='localhost:9092',
        group_id=None,
        auto_offset_reset='earliest'  # latest, earliest
    )

    # print(consumer.config)
    print(consumer.bootstrap_connected())

    # Pull messages every 1 second
    consumer.poll(timeout_ms=1000)

    for message in consumer:
        print(message)

except Exception as e:
    print(e)
    # Logs the error appropriately.
    pass
