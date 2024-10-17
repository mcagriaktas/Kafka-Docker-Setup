from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:19092', 'localhost:29092', 'localhost:39092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(3, 8, 0)
)

topic = 'cagri1'

for i in range(1000000):
    message = {'number': i, 'timestamp': time.time()}
    future = producer.send(topic, value=message)
    try:
        record_metadata = future.get(timeout=10)
        print(f"Message sent successfully: topic={record_metadata.topic}, partition={record_metadata.partition}, offset={record_metadata.offset}")
    except KafkaError as e:
        print(f"Failed to send message: {e}")
    time.sleep(1)

producer.flush()
producer.close()
