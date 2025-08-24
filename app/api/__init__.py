from fastapi import FastAPI
from kafka.producer import ingest_producer_url
from app.schema import IngestRequest


app = FastAPI(title="Youtube Pipeline Stream")


@app.post("/ingest")
async def ingest_url(payload: IngestRequest):
    return ingest_producer_url(payload)
