import random
import time
import uuid
from datetime import datetime, timezone
from faker import Faker

fake = Faker()

EVENT_TYPES = ['page_view', 'add_to_cart', 'checkout', 'search', 'product_click']
PAGES = ['/home', '/product/123', '/product/456', '/cart', '/checkout', '/search']

def generate_clickstream_event():
    return {
        'user_id': str(random.randint(1000, 1050)),  # small pool so same user repeats, realistic for session behavior
        'session_id': str(uuid.uuid4()),
        'event_type': random.choice(EVENT_TYPES),
        'page': random.choice(PAGES),
        'timestamp': datetime.now(timezone.utc).isoformat()
    }

if __name__ == '__main__':
    for _ in range(5):
        event = generate_clickstream_event()
        print(event)
        time.sleep(1)