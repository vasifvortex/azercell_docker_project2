from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka import avro

# Schema Registry and Kafka configuration
config = {
    'bootstrap.servers': '91.99.130.17:9094',
    'schema.registry.url': 'http://localhost:8081'
}

# Define the Avro schema (must match what was registered)
value_schema_str = """
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "name", "type": "string"}
  ]
}
"""

value_schema = avro.loads(value_schema_str)

# Initialize AvroProducer
producer = AvroProducer(config, default_value_schema=value_schema)

# Produce messages
for i in range(250,300):
    value = {"id": i, "name": f"User-{i}"}
    try:
        producer.produce(topic='testtopic', value=value)
        print(f"Sent: {value}")
    except SerializerError as e:
        print(f"Message serialization failed: {e}")

producer.flush()

