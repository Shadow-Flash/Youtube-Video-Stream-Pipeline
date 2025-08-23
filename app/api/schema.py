from pydantic import BaseModel
from pydantic_settings import BaseSettings


# Environment Variables
class EnvSettings(BaseSettings):
    bootstrap_server: str
    topic_ingest_url: str
    topic_media_audio_raw: str
    topic_media_transcripts: str
    topic_media_entities: str
    topic_media_chapters: str
    topic_media_summaries: str
    topic_media_moderation: str

    class Config:
        env_file = ".env"


# Ingest Url Request Model
class IngestRequest(BaseModel):
    url: str


__all__ = ["IngestRquest", "EnvSettings"]
