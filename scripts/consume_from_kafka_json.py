from kafka import KafkaConsumer
import json

# Define broker list
bootstrap_servers = ['44.200.188.188:9094']

# Create Kafka consumer
consumer = KafkaConsumer(
    'bigdata',
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',     # Start from the earliest message
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
    group_id='my-group'               # Specify a group ID
)

print("Waiting for messages... Press CTRL+C to stop.")

for message in consumer:
    print(f"Key: {message.key}, Value: {message.value}")