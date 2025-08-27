import json
import re
from collections import defaultdict, Counter

from sentence_transformers import SentenceTransformer, util
from sklearn.feature_extraction.text import CountVectorizer
from kafka.consumer import init_consumer
from app.schema import EnvSettings
from kafka.producer import build_kafka_producer

settings = EnvSettings()

consumer = init_consumer(
    settings.topic_media_transcripts,
    settings.group_id_transcript_2,
    settings.offset_latest,
)
producer = build_kafka_producer()

# --- NLP model ---
model = SentenceTransformer("all-MiniLM-L6-v2")

STOPWORDS = set(
    "a an and the is are was were be been it of on in to for with at by from this that these those".split()
)

SIM_THRESHOLD = 0.45  # if similarity drops below → new chapter


def get_title(text: str) -> str:
    """Extract top keyword for chapter title with safe fallback."""
    tokens = re.findall(r"\w+", text.lower())
    tokens = [t for t in tokens if t not in STOPWORDS]

    if not tokens:
        return "Untitled"

    try:
        vec = CountVectorizer(stop_words="english").fit([" ".join(tokens)])
        terms = vec.get_feature_names_out()
        if len(terms) == 0:
            return tokens[0]
        counts = Counter(tokens)
        terms_sorted = sorted(terms, key=lambda t: counts[t], reverse=True)
        return terms_sorted[0]
    except ValueError:
        # fallback to most common token
        return Counter(tokens).most_common(1)[0][0]


def get_summary(text: str, max_sentences=2) -> str:
    sentences = re.split(r"(?<=[.!?]) +", text)
    return " ".join(sentences[:max_sentences]).strip()


def emit_chapter(cid, chapter_id, sentences):
    start_ms = sentences[0]["start_ms"]
    end_ms = sentences[-1]["end_ms"]
    text = " ".join(s["text"] for s in sentences)

    out = {
        "cid": cid,
        "chapter_id": chapter_id,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "title": get_title(text),
        "summary": get_summary(text),
    }

    producer.produce(
        settings.topic_media_chapters, key=cid, value=json.dumps(out).encode("utf-8")
    )
    print(f"Produced chapter {chapter_id} for cid={cid}")


def main():
    print("Started consuming transcripts...")

    buffers = defaultdict(list)
    chapter_id_counter = defaultdict(int)
    for data in consumer:
        payload = json.loads(data.get("value").decode("utf-8"))
        cid = payload["cid"]

        # add sentence
        buffers[cid].append(payload)

        # check similarity with previous sentence
        if len(buffers[cid]) > 1:
            prev_text = buffers[cid][-2]["text"]
            curr_text = buffers[cid][-1]["text"]

            emb_prev = model.encode(prev_text, convert_to_tensor=True)
            emb_curr = model.encode(curr_text, convert_to_tensor=True)

            sim = util.cos_sim(emb_prev, emb_curr).item()

            if sim < SIM_THRESHOLD:
                # similarity drop → cut chapter
                chapter_id_counter[cid] += 1
                emit_chapter(cid, chapter_id_counter[cid], buffers[cid][:-1])
                # keep current as start of next chapter
                buffers[cid] = [buffers[cid][-1]]

        # flush leftover chapters
    for cid, sentences in buffers.items():
        if sentences:
            chapter_id_counter[cid] += 1
            emit_chapter(cid, chapter_id_counter[cid], sentences)

    producer.flush()
    print("NLP Chapterizer Completed !!")


if __name__ == "__main__":
    main()
