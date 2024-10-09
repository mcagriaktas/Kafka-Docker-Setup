from confluent_kafka import Consumer, KafkaError

# Configure the Confluent Kafka consumer
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest' 
}

consumer = Consumer(conf)

consumer.subscribe(['dahbest'])

print("Consuming messages from the 'dahbest' topic...")


while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(f"Consumer error: {msg.error()}")
            continue

    key = msg.key().decode('utf-8') if msg.key() else None
    value = msg.value().decode('utf-8') if msg.value() else None
    print(f"Consumed record with key: {key}, value: {value}, from partition: {msg.partition()} at offset: {msg.offset()}")

    consumer.close()

