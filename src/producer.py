import os
import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

# Decoupled Configuration via Environment Variables
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME", "raw-ecommerce-events")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

EVENT_TYPES = ['view_item', 'add_to_cart', 'remove_from_cart', 'purchase']
DEVICE_TYPES = ['iOS', 'Android', 'Web']

print(f"Streaming Clickstream Events to Kafka Topic: '{TOPIC_NAME}' via {KAFKA_SERVERS}")

try:
    while True:
        event = {
            "event_id": fake.uuid4(),
            "user_id": f"user_{random.randint(1000, 1050)}",
            "session_id": fake.uuid4(),
            "event_type": random.choices(EVENT_TYPES, weights=[60, 25, 5, 10])[0],
            "product_id": f"prod_{random.randint(100, 500)}",
            "category": random.choice(['Electronics', 'Footwear', 'Clothing', 'Books', 'Home']),
            "price": round(random.uniform(10.0, 500.0), 2),
            "device": random.choice(DEVICE_TYPES),
            "ip_address": fake.ipv4(),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        producer.send(TOPIC_NAME, value=event)
        producer.flush()
        
        print(f"Sent: {event['event_type']} by {event['user_id']} | Price: ${event['price']}")
        time.sleep(random.uniform(0.2, 0.5))

except KeyboardInterrupt:
    print("\nProducer stopped.")
finally:
    producer.flush()
    producer.close()
