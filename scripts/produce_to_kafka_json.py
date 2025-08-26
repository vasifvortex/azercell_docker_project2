from kafka import KafkaProducer
import json
import time

# Define broker list
bootstrap_servers = ['localhost:9092', 'localhost:9292', 'localhost:9392']

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None,
    retries=5
)

# Topic name
topic = 'bigdata'

# Send sample messages
for i in range(10):
    key = f'key-{i}'
    value = {"id": i, "message": f"Hello from Kafka {i}"}
    producer.send(topic, key=key, value=value)
    print(f"Sent: {value}")
    time.sleep(0.5)

# Finalize
producer.flush()
producer.close()

