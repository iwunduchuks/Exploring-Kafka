from confluent_kafka import Consumer, KafkaException, KafkaError

conf = {
    'bootstrap.servers': "localhost:9092",
    'group.id': "my_group",
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

def print_assignment(consumer, partitions):
    print('Assignment:', partitions)

consumer.subscribe(['my_topic'], on_assign=print_assignment)

while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break
    print(f"Received message: {msg.value().decode('utf-8')}")

consumer.close()
