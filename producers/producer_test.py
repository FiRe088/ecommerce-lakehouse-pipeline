from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:9093'}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")

producer.produce('clickstream-events', key='test-user', value='hello from confluent-kafka', callback=delivery_report)
producer.flush()