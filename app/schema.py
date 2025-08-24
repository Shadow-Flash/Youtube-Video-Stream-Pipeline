from typing import Any, Literal
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
    group_id_ingest_url: str
    group_id_audio_raw: str
    offset_latest: str
    offset_earliest: str
    sample_rate: int
    channels: int
    bytes_per_sample: int
    chunk_sec: float

    class Config:
        env_file = ".env"


# Ingest Url Request Model
class IngestRequest(BaseModel):
    url: str


# Ingest Url Response Model
class IngestResponse(BaseModel):
    status: str
    topic: str


class PipelineReturn(BaseModel):
    ytdlp: Any
    ffmpeg: Any
    stdout_pipe: Any


OffsetType = Literal["latest", "earliest"]


class ConsumerType(BaseModel):
    topic_name: str
    group_id: str
    offset: OffsetType


class ConsumerReturn(BaseModel):
    key: str
    value: str
