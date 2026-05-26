"""
batch/clickstream_batch.py
──────────────────────────
PySpark batch job that reads all downloaded clickstream TSV.GZ files,
applies schema enforcement and light cleaning, then computes:

  1. Top-N most linked-to articles  (in-degree)
  2. Top-N most linked-from articles (out-degree)
  3. Top internal link pairs by click count
  4. Monthly trend per article (when multiple months are loaded)

Output is written as partitioned Parquet to data/batch_output/.

Usage:
  # Download data first:
  python ingestion/download_clickstream.py

  # Then run this job:
  python batch/clickstream_batch.py

  # Or submit to a cluster:
  spark-submit --master yarn batch/clickstream_batch.py
"""

import sys
import os
from pathlib import Path

# Must be set before the JVM starts so the driver heap is large enough
# for aggregating 200M+ rows without RowBasedKeyValueBatch OOM.
os.environ.setdefault("SPARK_DRIVER_MEMORY", "4g")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config

# ── Schema ─────────────────────────────────────────────────────────────────────
# Wikimedia clickstream TSV columns (no header in file):
#   prev     – the referrer article title (or special token like "other-search")
#   curr     – the target article title
#   type     – "link" | "external" | "other"
#   n        – number of occurrences this month
CLICKSTREAM_SCHEMA = StructType([
    StructField("prev", StringType(), nullable=False),
    StructField("curr", StringType(), nullable=False),
    StructField("type", StringType(), nullable=False),
    StructField("n",    LongType(),   nullable=False),
])

TOP_N = 50  # number of rows to keep in each ranked output


# ── Helpers ────────────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"{config.SPARK_APP_NAME}-batch")
        .master(config.SPARK_MASTER)
        .config("spark.driver.memory", "4g")
        # More shuffle partitions → smaller per-task hash tables → less agg memory
        .config("spark.sql.shuffle.partitions", "400")
        # Give execution memory a bigger share; no caching so storage share can be minimal
        .config("spark.memory.fraction", "0.8")
        .config("spark.memory.storageFraction", "0.1")
        # Allow reading many small files efficiently
        .config("spark.sql.files.maxPartitionBytes", "128m")
        .getOrCreate()
    )


def load_clickstream(spark: SparkSession):
    """
    Read all TSV.GZ files in RAW_DATA_DIR.
    Adds a 'month' column extracted from the filename.
    """
    raw_path = str(Path(config.RAW_DATA_DIR).resolve())
    pattern = f"{raw_path}/clickstream-{config.CLICKSTREAM_WIKI}-*.tsv.gz"

    df = (
        spark.read
        .option("sep", "\t")
        .option("header", "false")
        .schema(CLICKSTREAM_SCHEMA)
        .csv(pattern)
    )

    # Extract YYYY-MM from the file path, e.g.
    # …/clickstream-enwiki-2024-10.tsv.gz  →  "2024-10"
    df = df.withColumn(
        "month",
        F.regexp_extract(F.input_file_name(), r"(\d{4}-\d{2})\.tsv\.gz$", 1)
    )
    return df


# ── Analyses ───────────────────────────────────────────────────────────────────

def top_articles_by_indegree(df):
    """Top N articles by total inbound click volume (all link types)."""
    return (
        df.groupBy("curr")
          .agg(F.sum("n").alias("total_clicks"))
          .orderBy(F.desc("total_clicks"))
          .limit(TOP_N)
    )


def top_articles_by_outdegree(df):
    """Top N articles that send the most clicks to other articles."""
    return (
        df.filter(F.col("type") == "link")   # internal links only
          .groupBy("prev")
          .agg(
              F.sum("n").alias("total_clicks_sent"),
              F.countDistinct("curr").alias("unique_targets"),
          )
          .orderBy(F.desc("total_clicks_sent"))
          .limit(TOP_N)
    )


def top_link_pairs(df):
    """Top N prev→curr pairs by total click count (internal links only)."""
    return (
        df.filter(F.col("type") == "link")
          .groupBy("prev", "curr")
          .agg(F.sum("n").alias("total_clicks"))
          .orderBy(F.desc("total_clicks"))
          .limit(TOP_N)
    )


def monthly_trend(df):
    """
    Monthly click volume per target article.
    Useful for plotting time-series for a specific article.
    """
    return (
        df.groupBy("month", "curr")
          .agg(F.sum("n").alias("monthly_clicks"))
          .orderBy("curr", "month")
    )


# ── Output ─────────────────────────────────────────────────────────────────────

def write(result_df, name: str, partition_by: list[str] | None = None):
    out = str(Path(config.BATCH_OUTPUT_DIR).resolve() / name)
    writer = result_df.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(out)
    print(f"[written] {out}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("Loading clickstream data …")
    df = load_clickstream(spark)
    # No cache — 212M rows won't fit in driver memory and cache eviction
    # competes with the aggregation hash tables (which can't spill).
    row_count = df.count()
    print(f"Loaded {row_count:,} rows across {df.select('month').distinct().count()} month(s)")

    print("Running analyses …")

    write(top_articles_by_indegree(df),  "top_articles_indegree")
    write(top_articles_by_outdegree(df), "top_articles_outdegree")
    write(top_link_pairs(df),            "top_link_pairs")
    write(monthly_trend(df),             "monthly_trend", partition_by=["month"])

    # Preview from already-written Parquet (avoids re-scanning 212M rows)
    print("\n── Top 10 articles by inbound clicks ──")
    spark.read.parquet(
        str(Path(config.BATCH_OUTPUT_DIR).resolve() / "top_articles_indegree")
    ).show(10, truncate=False)

    print("\n── Top 10 internal link pairs ──")
    spark.read.parquet(
        str(Path(config.BATCH_OUTPUT_DIR).resolve() / "top_link_pairs")
    ).show(10, truncate=False)

    spark.stop()
    print("Batch job complete.")


if __name__ == "__main__":
    main()
