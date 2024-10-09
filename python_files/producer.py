from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

conf = {
    'bootstrap.servers': '172.80.0.11:9092'
}

producer = Producer(conf)
for i in range(100):
    key = f'key-{i}'
    value = f'message-{i}'
    
    producer.produce('dahbest', key=key, value=value, callback=delivery_report)
    producer.poll(0)

producer.flush()
