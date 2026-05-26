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
        # Avoid INFO flood from Kafka consumer
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
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
        .load()
    )

    # Kafka value is bytes → decode to string → parse JSON
    parsed = (
        raw
        .select(F.from_json(F.col("value").cast("string"), EDIT_SCHEMA).alias("e"))
        .select("e.*")
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
    return parsed


# ── Aggregations ───────────────────────────────────────────────────────────────

def edits_per_wiki_per_minute(df):
    """1-minute tumbling window: edit count per wiki."""
    return (
        df
        .withWatermark("event_time", "2 minutes")   # late data tolerance
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
    return (
        df
        .filter(F.col("bot") == False)             # human edits only
        .withWatermark("event_time", "5 minutes")
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
    return (
        df
        .withWatermark("event_time", "2 minutes")
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
