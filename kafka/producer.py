from confluent_kafka import Producer
from app.schema import EnvSettings, IngestRequest, IngestResponse

settings = EnvSettings()
producer = Producer({"bootstrap.servers": settings.bootstrap_server})


def ingest_producer_url(payload: IngestRequest) -> IngestResponse:
    producer.produce(
        settings.topic_ingest_url,
        key="URL",
        value=payload.url.encode("utf-8"),
    )
    producer.flush()
    return IngestResponse(status="sent", topic=settings.topic_ingest_url)


def build_kafka_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.bootstrap_server,
            # tune as you like:
            "linger.ms": 10,
            "batch.num.messages": 1000,
            "message.max.bytes": 10_000_000,
        }
    )
