import random
import time
import uuid
import json
from datetime import datetime, timezone
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

EVENT_TYPES = ['page_view', 'add_to_cart', 'checkout', 'search', 'product_click']
PAGES = ['/home', '/product/123', '/product/456', '/cart', '/checkout', '/search']

def generate_clickstream_event():
    return {
        'user_id': str(random.randint(1000, 1050)),
        'session_id': str(uuid.uuid4()),
        'event_type': random.choice(EVENT_TYPES),
        'page': random.choice(PAGES),
        'timestamp': datetime.now(timezone.utc).isoformat()
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Sent to {msg.topic()} [partition {msg.partition()}] key={msg.key()}")

if __name__ == '__main__':
    conf = {'bootstrap.servers': 'localhost:9093'}
    producer = Producer(conf)

    for _ in range(15):
        event = generate_clickstream_event()
        producer.produce(
            'clickstream-events',
            key=event['user_id'],
            value=json.dumps(event),
            callback=delivery_report
        )
        producer.poll(0)
        time.sleep(1)

    producer.flush()