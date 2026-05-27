"""
streaming/kafka_to_spark.py
────────────────────────────
Spark Structured Streaming job that reads wiki-edit events from Kafka,
applies JSON parsing + schema enforcement, and computes:

  1. Edit rate per wiki (1-minute tumbling window)
  2. Top editors in a sliding window (5 min window, 1 min slide)
  3. New vs. edit vs. categorize breakdown (1-minute tumbling window)

Results are written as streaming Parquet sinks (append mode).

A separate console sink prints a live summary every trigger interval
so you can watch the pipeline without opening Parquet files.

Dependencies (installed automatically if using requirements.txt):
  pyspark>=3.5.0
  The Kafka connector JAR is fetched via --packages at spark-submit time.

Usage:
  spark-submit \\
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
    streaming/kafka_to_spark.py

  # Or for quick local dev (Spark downloads the package automatically):
  python streaming/kafka_to_spark.py
"""

import sys
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, BooleanType, IntegerType, TimestampType
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config

# ── Event schema (Wikimedia recentchange) ──────────────────────────────────────
# Full schema: https://stream.wikimedia.org/v2/stream/recentchange
# We select only the fields we need.
EDIT_SCHEMA = StructType([
    StructField("wiki",       StringType(),  True),
    StructField("title",      StringType(),  True),
    StructField("user",       StringType(),  True),
    StructField("type",       StringType(),  True),   # "edit","new","categorize"
    StructField("bot",        BooleanType(), True),
    StructField("timestamp",  LongType(),    True),   # Unix epoch seconds
    StructField("comment",    StringType(),  True),
    StructField("length",     StructType([  # nested: old + new byte lengths
        StructField("old", IntegerType(), True),
        StructField("new", IntegerType(), True),
    ]), True),
    StructField("revision",   StructType([
        StructField("old", LongType(), True),
        StructField("new", LongType(), True),
    ]), True),
    StructField("_ingested_at", StringType(), True),  # added by our producer
])


# ── Spark session ──────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(f"{config.SPARK_APP_NAME}-streaming")
        .master(config.SPARK_MASTER)
        # Pull the Kafka connector when running locally via python (not spark-submit)
        .config("spark.jars.packages", config.SPARK_KAFKA_PACKAGE)
        # Force loopback binding — avoids Windows hostname-resolution hangs that
        # prevent the BlockManager from registering during local-mode startup.
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        # ── Vectorised Parquet writes ─────────────────────────────────────────
        # Note: AQE (spark.sql.adaptive.enabled) is intentionally omitted here.
        # Spark 3.5 does not support AQE in streaming DataFrames and emits a
        # warning if it is set.  AQE is only enabled in the batch SparkSession.
        .config("spark.sql.parquet.columnarReaderBatchSize", "4096")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ── Source ─────────────────────────────────────────────────────────────────────

def read_kafka(spark: SparkSession):
    """Return a streaming DataFrame from Kafka, with the JSON payload parsed."""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.KAFKA_TOPIC_EDITS)
        .option("startingOffsets", "latest")
        # If the consumer falls too far behind Kafka will raise; cap the rate
        .option("maxOffsetsPerTrigger", 10_000)
        # If Kafka purges offsets that the checkpoint recorded (e.g. after a
        # broker restart with short retention), skip the missing data instead
        # of crashing.  This keeps the job alive across Docker restarts.
        .option("failOnDataLoss", "false")
        .load()
    )

    # Kafka value is bytes → decode to string → parse JSON
    parsed = (
        raw
        # Retain the Kafka message key alongside the parsed payload.
        # The producer sets key = b"{wiki}:{revision_id}", which is a globally
        # unique identifier for every edit event.  We use it below to deduplicate
        # re-delivered messages without hashing the full payload.
        .select(
            F.col("key"),
            F.from_json(F.col("value").cast("string"), EDIT_SCHEMA).alias("e"),
        )
        .select("key", "e.*")
        # Convert Unix epoch → proper Spark timestamp for windowing
        .withColumn("event_time", F.to_timestamp(F.col("timestamp")))
        # Derive byte-delta for size change
        .withColumn(
            "size_delta",
            F.col("length.new") - F.col("length.old")
        )
        # Drop null event_times (malformed events)
        .filter(F.col("event_time").isNotNull())
    )

    # ── Watermark-bounded deduplication ─────────────────────────────────────────
    # Kafka at-least-once delivery and SSE reconnects can produce duplicate
    # messages.  dropDuplicates on the message key (set by the producer to
    # "{wiki}:{revision_id}") removes re-delivered copies of the same edit.
    #
    # Spark 3.5 enforces a single watermark per query plan: calling
    # withWatermark more than once on the same column raises an
    # AnalysisException.  The watermark is therefore defined HERE (once) at
    # the widest tolerance needed by any downstream aggregation (5 minutes
    # for top_editors_sliding).  The aggregation functions must NOT call
    # withWatermark themselves — they inherit this definition.
    parsed = (
        parsed
        .withColumn("_kafka_key", F.col("key").cast("string"))
        .drop("key")
        .withWatermark("event_time", "5 minutes")
        .dropDuplicates(["_kafka_key"])
        .drop("_kafka_key")
    )

    return parsed


# ── Aggregations ───────────────────────────────────────────────────────────────

def edits_per_wiki_per_minute(df):
    """1-minute tumbling window: edit count per wiki."""
    # Watermark is already set upstream in read_kafka(); do not redefine it.
    return (
        df
        .groupBy(
            F.window("event_time", "1 minute"),
            "wiki",
        )
        .agg(
            F.count("*").alias("edit_count"),
            F.sum(F.when(F.col("bot") == True, 1).otherwise(0)).alias("bot_edits"),
            F.avg("size_delta").alias("avg_size_delta"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "wiki", "edit_count", "bot_edits", "avg_size_delta",
        )
    )


def top_editors_sliding(df):
    """5-minute sliding window (1 min slide): top editors by edit count."""
    # Watermark is already set upstream in read_kafka(); do not redefine it.
    return (
        df
        .filter(F.col("bot") == False)             # human edits only
        .groupBy(
            F.window("event_time", "5 minutes", "1 minute"),
            "user",
            "wiki",
        )
        .agg(F.count("*").alias("edit_count"))
        .select(
            F.col("window.start").alias("window_start"),
            "user", "wiki", "edit_count",
        )
    )


def edit_type_breakdown(df):
    """1-minute tumbling window: new / edit / categorize counts."""
    # Watermark is already set upstream in read_kafka(); do not redefine it.
    return (
        df
        .groupBy(
            F.window("event_time", "1 minute"),
            "type",
        )
        .agg(F.count("*").alias("count"))
        .select(
            F.col("window.start").alias("window_start"),
            "type", "count",
        )
    )


# ── Sinks ──────────────────────────────────────────────────────────────────────

def _parquet_sink(stream_df, name: str, trigger_seconds: int = 30):
    out_path = str(Path(config.STREAMING_OUTPUT_DIR).resolve() / name)
    chk_path = str(Path(config.STREAMING_CHECKPOINT_DIR).resolve() / name)
    Path(out_path).mkdir(parents=True, exist_ok=True)
    Path(chk_path).mkdir(parents=True, exist_ok=True)

    return (
        stream_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", out_path)
        .option("checkpointLocation", chk_path)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )


def _console_sink(stream_df, name: str, num_rows: int = 10):
    """Print to stdout for local dev monitoring."""
    chk_path = str(Path(config.STREAMING_CHECKPOINT_DIR).resolve() / f"{name}_console")
    Path(chk_path).mkdir(parents=True, exist_ok=True)

    return (
        stream_df.writeStream
        .format("console")
        .outputMode("update")
        .option("truncate", False)
        .option("numRows", num_rows)
        .option("checkpointLocation", chk_path)
        .trigger(processingTime="30 seconds")
        .start()
    )


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    spark = build_spark()

    print(f"Reading from Kafka topic '{config.KAFKA_TOPIC_EDITS}' …")
    edits = read_kafka(spark)

    # Build aggregated streams
    edits_by_wiki   = edits_per_wiki_per_minute(edits)
    top_editors     = top_editors_sliding(edits)
    type_breakdown  = edit_type_breakdown(edits)

    # Parquet sinks (durable output)
    q1 = _parquet_sink(edits_by_wiki,  "edits_per_wiki_per_minute")
    q2 = _parquet_sink(top_editors,    "top_editors_sliding")
    q3 = _parquet_sink(type_breakdown, "edit_type_breakdown")

    # Console sinks (local dev convenience)
    q4 = _console_sink(edits_by_wiki,  "edits_per_wiki_per_minute")

    print("Streaming queries started. Ctrl-C to stop.\n")
    print(f"  Parquet output:     {Path(config.STREAMING_OUTPUT_DIR).resolve()}")
    print(f"  Checkpoints:        {Path(config.STREAMING_CHECKPOINT_DIR).resolve()}\n")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nStopping all streaming queries …")
        for q in [q1, q2, q3, q4]:
            q.stop()

    spark.stop()
    print("Streaming job stopped.")


if __name__ == "__main__":
    main()
