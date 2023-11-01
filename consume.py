# Filename: consumer_app.py

from confluent_kafka import Consumer, KafkaException

def create_consumer():
    conf = {
        'bootstrap.servers': 'localhost:19092',
        'group.id': 'fraud-detection-group',
        'auto.offset.reset': 'earliest',  # consume from the beginning if no offset is stored for a consumer group
    }

    return Consumer(conf)

def consume_data(consumer, topic_name):
    consumer.subscribe([topic_name])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # timeout in seconds; adjust as needed
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event - not an error
                    print(f"{msg.topic()}:{msg.partition()}:{msg.offset()}: {msg.error()}")
                else:
                    raise KafkaException(msg.error())
            else:
                # Proper message
                print(f"{msg.topic()}:{msg.partition()}:{msg.offset()}: key={msg.key()}, value={msg.value()}")

    except KeyboardInterrupt:
        pass
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

if __name__ == "__main__":
    topic_name = 'fraud-detection'
    consumer = create_consumer()
    consume_data(consumer, topic_name)
