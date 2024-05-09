from kafka import KafkaConsumer
import json

try:
    print('Kafka Consumer!')

    consumer = KafkaConsumer(
        'dbserver2.inventory.customers',
        bootstrap_servers='localhost:9092',
        group_id=None,
        auto_offset_reset='latest',  # latest, earliest
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    # print(consumer.config)
    connected = consumer.bootstrap_connected()
    print(f"Connected: {connected}")
    print("\n")

    # Pull messages every 1 second
    # consumer.poll(timeout_ms=1000)

    print(f"Receiving data:")
    for message in consumer:
        print(message.value)

except Exception as e:
    print(e)
    # Logs the error appropriately.
    pass
