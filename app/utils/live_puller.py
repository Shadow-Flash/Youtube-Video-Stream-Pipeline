import math
import signal
import sys
import uuid
from subprocess import Popen, PIPE
from kafka.producer import build_kafka_producer
from kafka.consumer import init_consumer
from app.schema import EnvSettings, PipelineReturn

settings = EnvSettings()
BYTES_PER_SEC = settings.sample_rate * settings.channels * settings.bytes_per_sample
CHUNK_BYTES = int(BYTES_PER_SEC * settings.chunk_sec)  # 128_000


def main():
    data_for_consumer = init_consumer(
        topic_name=settings.topic_ingest_url,
        group_id=settings.group_id_ingest_url,
        offset=settings.offset_latest,
    )
    if "value" in data_for_consumer[0]:
        url = data_for_consumer[0].get("value")
        print(f"Received URL from consumer: {url}")
        publish_chunks(url)
    else:
        print("No message received from consumer.")
        sys.exit(1)


def run_pipeline(url: str) -> PipelineReturn:
    """
    Returns (ffmpeg_proc, stdout_pipe) where stdout_pipe yields raw PCM s16le 16k mono.
    """
    # yt-dlp: best audio to stdout
    ytdlp = Popen(
        ["yt-dlp", "-f", "bestaudio/best", "-o", "-", url],
        stdout=PIPE,
        stderr=sys.stderr,
        bufsize=0,
    )

    # ffmpeg: read from pipe, transcode to s16le 16k mono -> stdout
    ffmpeg = Popen(
        [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostats",
            "-i",
            "pipe:0",
            "-vn",
            "-ac",
            str(settings.channels),
            "-ar",
            str(settings.sample_rate),
            "-acodec",
            "pcm_s16le",
            "-f",
            "s16le",
            "pipe:1",
        ],
        stdin=ytdlp.stdout,
        stdout=PIPE,
        stderr=sys.stderr,
        bufsize=0,
    )

    # Ensure yt-dlp stdout is not held open in parent
    if ytdlp.stdout:
        ytdlp.stdout.close()

    return ytdlp, ffmpeg, ffmpeg.stdout


def publish_chunks(url: str):
    # correlation id
    cid = str(uuid.uuid4())

    producer = build_kafka_producer()
    ytdlp, ffmpeg, stdout_pipe = run_pipeline(url)

    seq = 0
    buffer = bytearray()
    chunk_ms = int(settings.chunk_sec * 1000)

    def cleanup():
        try:
            if stdout_pipe:
                stdout_pipe.close()
        except Exception:
            pass
        try:
            if ffmpeg.poll() is None:
                ffmpeg.terminate()
        except Exception:
            pass
        try:
            if ytdlp.poll() is None:
                ytdlp.terminate()
        except Exception:
            pass
        try:
            producer.flush(10)
        except Exception:
            pass

    def signal_handler(signum, frame):
        cleanup()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        while True:
            chunk = stdout_pipe.read(64 * 1024)  # read in 64KB blocks
            if not chunk:
                # end of stream
                break
            buffer.extend(chunk)

            while len(buffer) >= CHUNK_BYTES:
                frame = bytes(buffer[:CHUNK_BYTES])
                del buffer[:CHUNK_BYTES]

                start_ms = seq * chunk_ms
                end_ms = start_ms + chunk_ms

                headers = [
                    ("cid", cid.encode("utf-8")),
                    ("seq", str(seq).encode("utf-8")),
                    ("start_ms", str(start_ms).encode("utf-8")),
                    ("end_ms", str(end_ms).encode("utf-8")),
                ]

                producer.produce(
                    topic=settings.topic_media_audio_raw,
                    key=f"{cid}:{seq}".encode("utf-8"),
                    value=frame,  # raw PCM bytes
                    headers=headers,
                )
                # Optionally poll to drive delivery callbacks (not used here)
                producer.poll(0)
                seq += 1

        # Publish trailing partial (if any) with actual end_ms
        if buffer:
            actual_sec = len(buffer) / BYTES_PER_SEC
            actual_ms = int(math.ceil(actual_sec * 1000))
            start_ms = seq * chunk_ms
            end_ms = start_ms + actual_ms

            headers = [
                ("cid", cid.encode("utf-8")),
                ("seq", str(seq).encode("utf-8")),
                ("start_ms", str(start_ms).encode("utf-8")),
                ("end_ms", str(end_ms).encode("utf-8")),
            ]

            producer.produce(
                topic=settings.topic_media_audio_raw,
                key=f"{cid}:{seq}".encode("utf-8"),
                value=bytes(buffer),
                headers=headers,
            )

        producer.flush()  # ensure all messages delivered
        print(f"Done. Published {seq + (1 if buffer else 0)} chunk(s) with cid={cid}")

    finally:
        # Clean up subprocesses and pipes
        try:
            stdout_pipe.close()
        except Exception:
            pass
        try:
            if ffmpeg.poll() is None:
                ffmpeg.wait(timeout=3)
        except Exception:
            pass
        try:
            if ytdlp.poll() is None:
                ytdlp.wait(timeout=3)
        except Exception:
            pass


if __name__ == "__main__":
    main()
