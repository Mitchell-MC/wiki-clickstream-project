"""
Central configuration for the Wikipedia clickstream pipeline.
Edit these values to match your environment.
"""

# ── Clickstream batch ──────────────────────────────────────────────────────────
# Base URL for Wikimedia clickstream dumps
CLICKSTREAM_BASE_URL = "https://dumps.wikimedia.org/other/clickstream"

# Months to download, e.g. ["2024-01", "2024-02", ..., "2024-06"]
# (set to a single month for a quick test run)
CLICKSTREAM_MONTHS = [
    "2024-10",
    "2024-11",
    "2024-12",
    "2025-01",
    "2025-02",
    "2025-03",
]

# Which wiki to pull (enwiki is ~200-400 MB gzipped per month)
CLICKSTREAM_WIKI = "enwiki"

# Local directory where raw TSV.GZ files are saved
RAW_DATA_DIR = "data/raw"

# Local directory for Parquet output of batch jobs
BATCH_OUTPUT_DIR = "data/batch_output"

# ── Kafka ──────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC_EDITS = "wiki-edits"

# ── EventStreams SSE ───────────────────────────────────────────────────────────
# Wikimedia EventStreams endpoint (streams recentchange events for all wikis)
EVENTSTREAMS_URL = (
    "https://stream.wikimedia.org/v2/stream/recentchange"
)

# Filter to a single wiki; set to None to ingest all wikis
EVENTSTREAMS_WIKI_FILTER = "enwiki"

# ── Spark ──────────────────────────────────────────────────────────────────────
SPARK_APP_NAME = "WikiPipeline"
# For local dev use "local[*]"; point to your cluster master for production
SPARK_MASTER = "local[*]"

# Kafka package required by Spark Structured Streaming
# pyspark 3.5.4 / Spark 3.5.4 uses Scala 2.12
SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4"

# Streaming output path (checkpointing + sink)
STREAMING_CHECKPOINT_DIR = "data/streaming_checkpoint"
STREAMING_OUTPUT_DIR = "data/streaming_output"
