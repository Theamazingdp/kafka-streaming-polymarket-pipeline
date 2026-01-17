#consumers/simple_consumer.py
from kafka import KafkaConsumer
import json

print("Starting Simple Consumer")

consumer = KafkaConsumer(
    'asset-prices',
    bootstrap_servers=['localhost:9092'],
    value_deserializer = lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset = 'latest',
    group_id = 'test-consumer'
)

print("Connected to Kafka!")
print("Listening for messages... \n")

for message in consumer:
    data = message.value
    print(f"BTC: ${data['price']:,.2f} at {data['timestamp']}")