from __future__ import annotations

import json
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Deque, Dict, List, Tuple, Optional
from kafka.consumer import init_consumer
from app.schema import EnvSettings
from kafka.producer import build_kafka_producer

settings = EnvSettings()

# NLP deps (lazy-loaded below)
import importlib


def iso(ts_sec: float) -> str:
    return datetime.fromtimestamp(ts_sec, tz=timezone.utc).isoformat()


def to_epoch_seconds(ts_value, fallback_sec: float) -> float:
    """Parse various timestamp formats to epoch seconds."""
    if ts_value is None:
        return float(fallback_sec)
    # numeric (sec or ms)
    try:
        v = float(ts_value)
        # heuristic: if it's too large, it's ms
        if v > 1e12:  # nanoseconds (unlikely)
            return v / 1e9
        if v > 1e10:  # milliseconds
            return v / 1e3
        return v
    except Exception:
        pass
    # ISO 8601
    try:
        dt = datetime.fromisoformat(str(ts_value).replace("Z", "+00:00"))
        return dt.timestamp()
    except Exception:
        return float(fallback_sec)


@dataclass
class WindowBuffer:
    items: Deque[Tuple[float, str]]
    last_emit_event_ts: float

    def __init__(self):
        self.items = deque()
        self.last_emit_event_ts = 0.0

    def add(self, ts_sec: float, text: str, horizon_sec: int):
        self.items.append((ts_sec, text))
        # prune strictly older than (latest_ts - horizon)
        cutoff = ts_sec - horizon_sec
        while self.items and self.items[0][0] < cutoff:
            self.items.popleft()

    def window_bounds(self) -> Tuple[Optional[float], Optional[float]]:
        if not self.items:
            return None, None
        # items may be slightly out-of-order; compute bounds across all
        t_min = min(t for t, _ in self.items)
        t_max = max(t for t, _ in self.items)
        return t_min, t_max

    def concatenated_text(self) -> str:
        # stable order by timestamp, then insertion
        sorted_items = sorted(self.items, key=lambda x: x[0])
        return " ".join(txt for _, txt in sorted_items)


def load_spacy_ner():
    import spacy

    try:
        # try dedicated package first (faster import)
        nlp = spacy.load(
            "en_core_web_sm", disable=["tagger", "parser", "lemmatizer", "textcat"]
        )
    except Exception:
        # final fallback: try to download
        try:
            from spacy.cli import download

            download("en_core_web_sm")
            nlp = spacy.load(
                "en_core_web_sm", disable=["tagger", "parser", "lemmatizer", "textcat"]
            )
        except Exception as e:
            raise RuntimeError(
                "spaCy model 'en_core_web_sm' not available. Install via: python -m spacy download en_core_web_sm"
            ) from e
    return nlp


def load_keyword_extractor(algo: str, max_keywords: int):
    algo = (algo or "yake").lower()
    if algo == "keybert":
        try:
            KeyBERT = importlib.import_module("keybert").KeyBERT
            kb = KeyBERT()

            def extract(text: str) -> List[Tuple[str, float]]:
                if not text.strip():
                    return []
                # MMR for diversity; returns (term, score) with higher=better
                kws = kb.extract_keywords(
                    text,
                    keyphrase_ngram_range=(1, 2),
                    stop_words="english",
                    top_n=max_keywords,
                    use_mmr=True,
                    diversity=0.5,
                )
                return [(k, float(s)) for k, s in kws]

            return extract, "keybert"
        except Exception:
            print("KeyBERT unavailable; falling back to YAKE", file=sys.stderr)
            # fall through to YAKE
    # YAKE
    try:
        yake = importlib.import_module("yake")
        extractor = yake.KeywordExtractor(lan="en", n=2, top=max_keywords)

        def extract(text: str) -> List[Tuple[str, float]]:
            if not text.strip():
                return []
            pairs = extractor.extract_keywords(
                text
            )  # List[(term, score)], lower=better
            if not pairs:
                return []
            # Normalize YAKE scores to 0..1 with higher=better
            scores = [s for _, s in pairs]
            s_min, s_max = min(scores), max(scores)
            norm = []
            for term, s in pairs:
                if s_max == s_min:
                    norm_score = 1.0
                else:
                    norm_score = 1.0 - ((s - s_min) / (s_max - s_min))
                norm.append((term, float(norm_score)))
            return norm[:max_keywords]

        return extract, "yake"
    except Exception as e:
        raise RuntimeError(
            "Neither KeyBERT nor YAKE are available. Install one of them."
        ) from e


def main():
    consumer = init_consumer(
        settings.topic_media_transcripts,
        settings.group_id_transcript,
        settings.offset_latest,
    )
    producer = build_kafka_producer()

    buffers: Dict[str, WindowBuffer] = defaultdict(WindowBuffer)
    nlp = load_spacy_ner()
    extract_keywords, algo_used = load_keyword_extractor("yake", 10)
    print(f"Keyword algorithm: {algo_used}")

    for data in consumer:
        payload = json.loads(data.get("value").decode("utf-8"))
        cid = str(payload.get("cid") or "default")
        text = str(payload.get("text") or "").strip()
        evt_ts = to_epoch_seconds(payload.get("end_ms"), time.time())
        buf = buffers[cid]
        buf.add(evt_ts, text, 90)
        if (evt_ts - buf.last_emit_event_ts) < 5:
            continue

        w_start, w_end = buf.window_bounds()
        if w_start is None or w_end is None:
            continue

        concat_text = buf.concatenated_text()

        # Extract entities
        doc = nlp(concat_text)
        entities: List[Tuple[str, str]] = []
        seen = set()
        for ent in doc.ents:
            key = (ent.text.strip(), ent.label_)
            if key not in seen and key[0]:
                seen.add(key)
                entities.append([key[0], key[1]])

        # Extract keywords
        try:
            kw_pairs = extract_keywords(concat_text)
        except Exception:
            kw_pairs = []

        out = {
            "cid": cid,
            "t_window": [iso(w_start), iso(w_end)],
            "keywords": [[str(term), float(score)] for term, score in kw_pairs],
            "entities": entities,
        }

        # --- Produce to Kafka ---
        producer.produce(
            settings.topic_media_entities,
            key=cid,
            value=json.dumps(out).encode("utf-8"),
        )

        buf.last_emit_event_ts = evt_ts

    # Ensure delivery
    producer.flush()
    print("NLP Entities Completed !!")


if __name__ == "__main__":
    main()
