import io
import wave
import json
from uuid import uuid4
from kafka.consumer import init_consumer
from app.schema import EnvSettings
from faster_whisper import WhisperModel
from kafka.producer import build_kafka_producer

settings = EnvSettings()


def convert_to_wav(pcmBytes: bytes):
    wav_buffer = io.BytesIO()
    with wave.open(wav_buffer, "wb") as wav_file:
        wav_file.setnchannels(settings.channels)
        wav_file.setsampwidth(settings.bytes_per_sample)
        wav_file.setframerate(settings.sample_rate)
        wav_file.writeframes(pcmBytes)
    wav_buffer.seek(0)
    return wav_buffer


def transform_to_transcript(audio):
    print("Started transforming data..")
    model = WhisperModel("small", device="cpu", compute_type="int8")
    segments, info = model.transcribe(audio, beam_size=5)
    producer = build_kafka_producer()
    lang = info.language
    for i, segment in enumerate(segments):
        cid = str(uuid4())
        detail = json.dumps(
            {
                "cid": cid,
                "seq": i,
                "start_ms": int(segment.start * 1000),
                "end_ms": int(segment.end * 1000),
                "text": segment.text.strip(),
                "lang": lang,
                "confidence": segment.avg_logprob,
            }
        )
        producer.produce(
            topic=settings.topic_media_transcripts,
            key=f"{cid}:{i}".encode("utf-8"),
            value=detail.encode("utf-8"),
        )
        producer.flush()
    print("Transformation complete.")


def main():
    data_for_consumer = init_consumer(
        topic_name=settings.topic_media_audio_raw,
        group_id=settings.group_id_audio_raw,
        offset=settings.offset_latest,
    )

    waveByte: bytes = b""
    for data_consume in data_for_consumer:
        if "value" in data_consume:
            waveByte += data_consume.get("value")
        else:
            print("No message received from consumer.")
            break
    waveFile = convert_to_wav(waveByte)
    transform_to_transcript(waveFile)


if __name__ == "__main__":
    main()
