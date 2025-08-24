from confluent_kafka.admin import AdminClient, NewTopic
from app.schema import EnvSettings

settings = EnvSettings()

# Connect to Kafka via the external listener (host)
BOOTSTRAP_SERVERS = settings.bootstrap_server

# Topics to create (name, partitions, replication_factor)
TOPICS = [
    (settings.topic_ingest_url, 3, 1),
    (settings.topic_media_audio_raw, 2, 1),
    (settings.topic_media_chapters, 1, 1),
    (settings.topic_media_entities, 4, 1),
    (settings.topic_media_moderation, 2, 1),
    (settings.topic_media_summaries, 2, 1),
    (settings.topic_media_transcripts, 2, 1),
]


def create_topics():
    admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    # Build NewTopic objects
    new_topics = [
        NewTopic(topic=name, num_partitions=partitions, replication_factor=rf)
        for name, partitions, rf in TOPICS
    ]

    # Submit creation requests
    fs = admin_client.create_topics(new_topics)

    # Collect results
    for topic, f in fs.items():
        try:
            f.result()  # raises exception if creation failed
            print(f"✅ Topic '{topic}' created successfully")
        except Exception as e:
            print(f"❌ Failed to create topic '{topic}': {e}")


if __name__ == "__main__":
    create_topics()
