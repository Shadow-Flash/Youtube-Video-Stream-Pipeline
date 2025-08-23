from fastapi import FastAPI
from app.api.router.ingest_api import router as ingest_router

app = FastAPI(title="Youtube Pipeline Stream")

# Register routers
app.include_router(ingest_router)
