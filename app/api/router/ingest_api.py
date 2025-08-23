from fastapi import APIRouter
from confluent_kafka import Producer
from app.api.schema import IngestRequest, EnvSettings

settings = EnvSettings()
router = APIRouter(prefix="/", tags=["Ingest_API"])
producer = Producer({"bootstrap.servers": settings.bootstrap_server})


@router.post("ingest")
async def ingest_url(payload: IngestRequest):
    producer.produce(settings.topic_ingest_url, str(payload).encode("utf-8"))
    producer.flush()
    return {"status": "sent", "topic": settings.topic_ingest_url}
