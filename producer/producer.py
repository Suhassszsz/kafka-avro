from confluent_kafka.avro import AvroProducer
from confluent_kafka import avro
import os


schema_path = os.path.join(os.path.dirname(__file__), '..', 'schema', 'user.avsc')
value_schema = avro.load(schema_path)

conf = {
    'bootstrap.servers': 'localhost:9092',
    'schema.registry.url': 'http://localhost:8081'
}

producer = AvroProducer(conf, default_value_schema=value_schema)


users = [
    {"user_id": 1, "name": "Alice", "email": "alice@example.com"},
    {"user_id": 2, "name": "Bob", "email": "bob@example.com"},
    {"user_id": 3, "name": "Charlie", "email": "charlie@example.com"}
]

topic = "users-avro"

for user in users:
    producer.produce(topic=topic, value=user)
    print(f"✅ Produced: {user}")

producer.flush()
print("🚀 All messages produced successfully.")
