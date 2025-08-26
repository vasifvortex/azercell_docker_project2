from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka import avro
import random

# Kafka and Schema Registry configuration
config = {
    'bootstrap.servers': '91.99.130.17:9094',
    'schema.registry.url': 'http://localhost:8081'
}

# Define the Avro schema for location data
value_schema_str = """
{
  "type": "record",
  "name": "Location",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "latitude", "type": "double"},
    {"name": "longitude", "type": "double"}
  ]
}
"""

value_schema = avro.loads(value_schema_str)

# Initialize AvroProducer
producer = AvroProducer(config, default_value_schema=value_schema)

# Produce messages with random coordinates
for i in range(250, 300):
    value = {
        "id": i,
        "latitude": random.uniform(-90.0, 90.0),
        "longitude": random.uniform(-180.0, 180.0)
    }
    try:
        producer.produce(topic='testtopic', value=value)
        print(f"Sent: {value}")
    except SerializerError as e:
        print(f"Message serialization failed: {e}")

producer.flush()
