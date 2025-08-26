from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError

# Consumer configuration
config = {
    'bootstrap.servers': '91.99.130.17:9094',
    'group.id': 'avro-consumer-group',
    'auto.offset.reset': 'earliest',
    'schema.registry.url': 'http://localhost:8081'
}

# Initialize AvroConsumer
consumer = AvroConsumer(config)
consumer.subscribe(['test_topic'])

print("Consuming messages from 'test_topic'...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error:", msg.error())
            continue

        print("Received:", msg.value())

except KeyboardInterrupt:
    pass
finally:
    consumer.close()

