from kafka import KafkaConsumer
import json
import time

# Kafka broker list
bootstrap_servers = ['localhost:9094', 'localhost:9095', 'localhost:9096']

# Create Kafka consumer
consumer = KafkaConsumer(
    'John_Snow',
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='sensor-consumers',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None
)

print("Listening to messages...")

for message in consumer:
    print(f"Received: key={message.key}, value={message.value}")
    time.sleep(3)
