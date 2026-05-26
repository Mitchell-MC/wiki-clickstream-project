"""
ingestion/sse_to_kafka.py
─────────────────────────
Tails the Wikimedia EventStreams SSE feed for recentchange events and
publishes each event as a JSON message to a Kafka topic.

Architecture:
  Wikimedia SSE  →  this script  →  Kafka topic (wiki-edits)
                                          ↓
                                   Spark Structured Streaming

The SSE feed reconnects automatically on network errors.  The Last-Event-ID
header is sent on reconnect so no events are missed (within the server's
30-minute replay window).

Dependencies:
  pip install sseclient-py kafka-python

Usage:
  # Start Kafka first (see docker/docker-compose.yml), then:
  python ingestion/sse_to_kafka.py
"""

import json
import sys
import time
import signal
import logging
from pathlib import Path
from datetime import datetime, timezone

import sseclient
import urllib.request

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Kafka producer setup ───────────────────────────────────────────────────────

def _make_producer(retries: int = 10, delay: float = 5.0) -> KafkaProducer:
    """Create a KafkaProducer, retrying until Kafka is ready."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                # Serialize dicts → UTF-8 JSON bytes
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                # Wait for leader acknowledgement only (good for high throughput)
                acks=1,
                # Compress messages to reduce network/disk usage
                compression_type="gzip",
                # Retry on transient send errors
                retries=5,
            )
            log.info("Connected to Kafka at %s", config.KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            log.warning(
                "Kafka not available (attempt %d/%d). Retrying in %.0fs…",
                attempt,
                retries,
                delay,
            )
            time.sleep(delay)
    raise RuntimeError(
        f"Could not connect to Kafka at {config.KAFKA_BOOTSTRAP_SERVERS} "
        f"after {retries} attempts."
    )


# ── SSE ingestion ──────────────────────────────────────────────────────────────

def _open_sse_stream(last_event_id: str | None = None):
    """
    Open the Wikimedia EventStreams SSE connection.
    Pass last_event_id to resume from where we left off.
    """
    headers = {
        "Accept": "text/event-stream",
        # Wikimedia requires a descriptive User-Agent for all API/stream access
        "User-Agent": "wiki-clickstream-pipeline/1.0 (https://github.com/Mitchell-MC/wiki-clickstream-project)",
    }
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id

    req = urllib.request.Request(config.EVENTSTREAMS_URL, headers=headers)
    response = urllib.request.urlopen(req, timeout=60)
    return sseclient.SSEClient(response)


def _should_keep(event_data: dict) -> bool:
    """Return True if the event passes the configured wiki filter."""
    if config.EVENTSTREAMS_WIKI_FILTER is None:
        return True
    return event_data.get("wiki") == config.EVENTSTREAMS_WIKI_FILTER


def _enrich(event_data: dict) -> dict:
    """Add pipeline metadata fields to the event."""
    event_data["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    return event_data


def run(producer: KafkaProducer):
    """Main loop: consume SSE → publish to Kafka indefinitely."""
    last_event_id: str | None = None
    total_published = 0
    total_filtered = 0

    while True:
        try:
            log.info("Opening SSE stream (last_event_id=%s)", last_event_id)
            client = _open_sse_stream(last_event_id)

            for event in client.events():
                # Track reconnect position
                if event.id:
                    last_event_id = event.id

                # SSE keep-alives have no data
                if not event.data or event.data.strip() == "":
                    continue

                try:
                    data = json.loads(event.data)
                except json.JSONDecodeError:
                    continue

                if not _should_keep(data):
                    total_filtered += 1
                    continue

                enriched = _enrich(data)

                # Set a deterministic Kafka message key so that:
                #   1. All events for the same article are routed to the same
                #      partition (ordering guarantee within a wiki+revision).
                #   2. Consumer-side deduplication can use this key as a
                #      globally-unique event identifier without content hashing.
                # Key format: b"{wiki}:{new_revision_id}" when available;
                # falls back to b"{wiki}:{timestamp}:{title}" for events
                # (e.g. log entries) that carry no revision object.
                revision = enriched.get("revision") or {}
                rev_new  = revision.get("new") if isinstance(revision, dict) else None
                wiki_id  = enriched.get("wiki", "")
                if rev_new and wiki_id:
                    msg_key = f"{wiki_id}:{rev_new}".encode("utf-8")
                else:
                    ts    = enriched.get("timestamp", "")
                    title = enriched.get("title", "")
                    msg_key = f"{wiki_id}:{ts}:{title}".encode("utf-8")

                producer.send(config.KAFKA_TOPIC_EDITS, key=msg_key, value=enriched)
                total_published += 1

                if total_published % 500 == 0:
                    log.info(
                        "Published %d events (filtered %d)",
                        total_published,
                        total_filtered,
                    )

        except KeyboardInterrupt:
            log.info("Shutting down (published %d events total)", total_published)
            break
        except Exception as exc:
            log.warning("Stream error: %s — reconnecting in 5s", exc)
            time.sleep(5)

    producer.flush()
    log.info("Producer flushed. Exiting.")


# ── entry point ────────────────────────────────────────────────────────────────

def main():
    producer = _make_producer()

    # Graceful shutdown on Ctrl-C or SIGTERM
    def _shutdown(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _shutdown)

    try:
        run(producer)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
