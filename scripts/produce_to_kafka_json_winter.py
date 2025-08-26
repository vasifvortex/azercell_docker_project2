from kafka import KafkaProducer
import json
import time
import random

# Kafka broker list
bootstrap_servers = ['100.27.158.24:9094', '100.27.158.24:9095', '100.27.158.24:9096']

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None,
    retries=5
)

topic = 'John_Snow'

# Simulate IoT sensor data
for i in range(20):
    key = f'sensor-{i % 3}'  # key for partitioning
    temp = round(random.uniform(-30.0, 30.0), 2)
    value = {
        "sensor_id": i % 3,
        "temperature": temp,
        "humidity": round(random.uniform(40.0, 60.0), 2),
        "timestamp": time.time(),
        "message": "Winter is coming!" if temp < 0 else "It is not below 0."
    }
    producer.send(topic, key=key, value=value)
    print(f"Sent: {value}")
    time.sleep(3)

producer.flush()
producer.close()

