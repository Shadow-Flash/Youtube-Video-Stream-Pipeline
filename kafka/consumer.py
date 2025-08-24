from confluent_kafka import Consumer, KafkaException
from app.schema import EnvSettings, ConsumerReturn, OffsetType

settings = EnvSettings()


def init_consumer(
    topic_name: str, group_id: str, offset: OffsetType
) -> ConsumerReturn | None:
    conf = {
        "bootstrap.servers": settings.bootstrap_server,
        "group.id": group_id,
        "auto.offset.reset": offset,
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic_name])
    msg_list = []

    print(f"✅ Listening for messages on topic '{topic_name}'...")
    try:
        while True:
            msg = consumer.poll(5.0)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    print(f"End of partition for {topic_name} reached.")
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break
            else:
                # Process the received message
                msg_list.append(
                    {
                        "key": msg.key().decode("utf-8") if msg.key() else None,
                        "value": (
                            msg.value()
                            if isinstance(msg.value(), bytes)
                            else msg.value().decode("utf-8")
                        ),
                        "headers": msg.headers() if msg.headers() else None,
                    }
                )

    except KeyboardInterrupt:
        print("Consumer stopped by user.")
    finally:
        consumer.close()
        print("Consumer closed.")
        return msg_list
