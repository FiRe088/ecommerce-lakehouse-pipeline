import random
import time
import uuid
from datetime import datetime, timezone
from faker import Faker

fake = Faker()

STATUS_FLOW = ['placed', 'paid', 'shipped', 'delivered']
PRODUCTS = ['sku-1001', 'sku-1002', 'sku-1003', 'sku-1004', 'sku-1005']

open_orders = {}  # order_id -> current status index

def create_new_order():
    order_id = str(uuid.uuid4())
    open_orders[order_id] = 0
    return {
        'order_id': order_id,
        'user_id': str(random.randint(1000, 1050)),
        'status': STATUS_FLOW[0],
        'amount': round(random.uniform(10.0, 500.0), 2),
        'items': random.sample(PRODUCTS, k=random.randint(1, 3)),
        'timestamp': datetime.now(timezone.utc).isoformat()
    }

def advance_existing_order():
    order_id = random.choice(list(open_orders.keys()))
    current_index = open_orders[order_id]

    # 10% chance an in-progress order gets cancelled instead of advancing
    if random.random() < 0.1:
        del open_orders[order_id]
        status = 'cancelled'
    else:
        next_index = current_index + 1
        status = STATUS_FLOW[next_index]
        if next_index == len(STATUS_FLOW) - 1:
            del open_orders[order_id]  # delivered = order lifecycle complete
        else:
            open_orders[order_id] = next_index

    return {
        'order_id': order_id,
        'status': status,
        'timestamp': datetime.now(timezone.utc).isoformat()
    }

def generate_order_event():
    # 40% chance of a new order, 60% chance of advancing an existing one (if any exist)
    if not open_orders or random.random() < 0.4:
        return create_new_order()
    else:
        return advance_existing_order()

if __name__ == '__main__':
    for _ in range(15):
        event = generate_order_event()
        print(event)
        time.sleep(1)