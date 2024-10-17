from confluent_kafka import Producer
import json
import time

producer_conf = {
    'bootstrap.servers': 'localhost:19092,localhost:29092,localhost:39092',
    'client.id': 'my-python-producer',
    'acks': 'all'
}

producer = Producer(producer_conf)

topic = 'cagri'

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

for i in range(1000000):
    message = {'number': i, 'timestamp': time.time()}  
    producer.produce(topic, value=json.dumps(message), callback=delivery_report)
    producer.poll(1)

producer.flush()
