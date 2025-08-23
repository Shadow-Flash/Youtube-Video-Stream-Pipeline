import json
from confluent_kafka import Consumer, KafkaException

BOOTSTRAP_SERVERS = "localhost:9094"  # adjust if needed
TOPIC = "ingest.requests"
GROUP_ID = "ingest-consumer-group"


def main():
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",  # consume from beginning
    }

    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])

    print(f"✅ Listening for messages on topic '{TOPIC}'...")

    try:
        while True:
            msg = consumer.poll(1.0)  # wait up to 1s
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            key = msg.key().decode("utf-8") if msg.key() else None
            value = msg.value().decode("utf-8")

            try:
                payload = json.loads(value.replace("'", '"'))  # fix str→dict encoding
            except Exception:
                payload = value

            print(f"📩 Received message | key={key} | value={payload}")

    except KeyboardInterrupt:
        print("👋 Shutting down consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
