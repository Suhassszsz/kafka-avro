from confluent_kafka.avro import AvroConsumer

conf = {
    'bootstrap.servers': 'localhost:9092',
    'schema.registry.url': 'http://localhost:8081',
    'group.id': 'avro-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = AvroConsumer(conf)
consumer.subscribe(["users-avro"])

print("📥 Waiting for messages...")
try:
    while True:
        msg = consumer.poll(5)
        if msg is None:
            continue
        if msg.error():
            print("❌ Consumer error:", msg.error())
            continue
        print("📨 Consumed:", msg.value())

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
