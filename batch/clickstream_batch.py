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
# Note: PYSPARK_DRIVER_MEMORY (not SPARK_DRIVER_MEMORY) is what PySpark reads
# when launching the py4j gateway JVM with `python script.py`.
os.environ.setdefault("PYSPARK_DRIVER_MEMORY", "4g")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType
)
from pyspark.sql.window import Window

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config
from ingestion.data_quality import validate_and_quarantine

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
        # ── Idempotency: dynamic partition overwrite ───────────────────────────
        # Only partitions present in the DataFrame being written are replaced.
        # All other historical partitions are left untouched, making every
        # batch write safe to retry without corrupting prior months.
        .config(
            "spark.sql.sources.partitionOverwriteMode",
            config.BATCH_PARTITION_OVERWRITE_MODE,
        )
        # ── Adaptive Query Execution (AQE) ────────────────────────────────────
        # Dynamically coalesces post-shuffle partitions, optimises skewed joins,
        # and switches join strategies at runtime — critical for clickstream data
        # where a tiny set of hub articles (e.g. "United_States") accounts for
        # a disproportionate share of edges.
        .config("spark.sql.adaptive.enabled",                    "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled",           "true")
        # ── Vectorised Parquet reads ──────────────────────────────────────────
        # Reads column batches instead of one row at a time, fully using
        # CPU SIMD instructions for aggregation operations.
        .config("spark.sql.parquet.columnarReaderBatchSize", "4096")
        # ── Dynamic resource allocation ───────────────────────────────────────
        # On a real cluster, executors spin up for the processing window and
        # are released immediately after — eliminating idle compute costs.
        # These settings are no-ops in local[*] mode.
        .config("spark.dynamicAllocation.enabled",           "true")
        .config("spark.dynamicAllocation.minExecutors",      "1")
        .config("spark.dynamicAllocation.maxExecutors",      "20")
        .config("spark.dynamicAllocation.executorIdleTimeout", "300s")
        # More shuffle partitions → smaller per-task hash tables → less agg memory
        .config("spark.sql.shuffle.partitions", "400")
        # Give execution memory a bigger share; no caching so storage share can be minimal
        .config("spark.memory.fraction", "0.8")
        .config("spark.memory.storageFraction", "0.1")
        # Allow the shuffle sorter and hash-agg to allocate off-heap pages so
        # the JVM heap is not exhausted during the (month, prev, curr) groupBy
        # which can produce tens of millions of intermediate rows.
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size",    "2g")
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
# All aggregations are grouped by "month" so output is partitioned per batch
# identifier.  Combined with dynamic partition overwrite, re-running any single
# month replaces only that month's Parquet partition — never touching others.
# Top-N ranking uses a window function (row_number) scoped per month so each
# month gets its own independent leaderboard.

def top_articles_by_indegree(df):
    """Top N articles per month by total inbound click volume (all link types)."""
    window = Window.partitionBy("month").orderBy(F.desc("total_clicks"))
    return (
        df.groupBy("month", "curr")
          .agg(F.sum("n").alias("total_clicks"))
          .withColumn("_rank", F.row_number().over(window))
          .filter(F.col("_rank") <= TOP_N)
          .drop("_rank")
    )


def top_articles_by_outdegree(df):
    """Top N articles per month that send the most clicks to other articles."""
    window = Window.partitionBy("month").orderBy(F.desc("total_clicks_sent"))
    return (
        df.filter(F.col("type") == "link")   # internal links only
          .groupBy("month", "prev")
          .agg(
              F.sum("n").alias("total_clicks_sent"),
              F.countDistinct("curr").alias("unique_targets"),
          )
          .withColumn("_rank", F.row_number().over(window))
          .filter(F.col("_rank") <= TOP_N)
          .drop("_rank")
    )


def top_link_pairs(df):
    """Top N prev→curr pairs per month by total click count (internal links only)."""
    window = Window.partitionBy("month").orderBy(F.desc("total_clicks"))
    return (
        df.filter(F.col("type") == "link")
          .groupBy("month", "prev", "curr")
          .agg(F.sum("n").alias("total_clicks"))
          .withColumn("_rank", F.row_number().over(window))
          .filter(F.col("_rank") <= TOP_N)
          .drop("_rank")
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

def write(
    result_df,
    name: str,
    partition_by: list[str] | None = None,
    sort_by: list[str] | None = None,
):
    """
    Write *result_df* to Parquet under BATCH_OUTPUT_DIR/<name>.

    Idempotency guarantee
    ---------------------
    With BATCH_PARTITION_OVERWRITE_MODE = "dynamic" (configured in SparkSession),
    Spark replaces only the partitions present in *result_df*.  Historical
    partitions that are not part of this run are left intact.  A mid-run
    failure followed by a retry is therefore safe: the retry simply overwrites
    the same partitions that the failed run was writing.

    Clustering / sort
    -----------------
    *sort_by* triggers sortWithinPartitions before writing.  Sorting by
    high-cardinality predicates (source_article = prev, referrer_type = type)
    groups related rows into the same Parquet row-groups, so downstream queries
    that filter on those columns skip irrelevant row-groups entirely
    (predicate push-down / min-max statistics).
    """
    out = str(Path(config.BATCH_OUTPUT_DIR).resolve() / name)
    df_to_write = (
        result_df.sortWithinPartitions(*sort_by) if sort_by else result_df
    )
    writer = df_to_write.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(out)
    print(f"[written] {out}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("Loading clickstream data …")
    raw_df = load_clickstream(spark)

    # ── Data quality gate (Bronze → Silver) ───────────────────────────────────
    # Validate every record against the data contract before any aggregation.
    # Violating records are quarantined to QUARANTINE_DIR and never reach the
    # analytics layer; the returned clean_df is guaranteed contract-compliant.
    print("Running data quality validation …")
    clean_df, quarantine_count = validate_and_quarantine(raw_df)

    months_loaded = sorted(
        row.month for row in clean_df.select("month").distinct().collect()
    )
    row_count = clean_df.count()
    print(
        f"Loaded {row_count:,} clean rows across "
        f"{len(months_loaded)} month(s): {', '.join(months_loaded)}"
        + (f"  |  {quarantine_count:,} quarantined" if quarantine_count else "")
    )

    # ── Analyses ───────────────────────────────────────────────────────────────
    print("Running analyses …")

    # All outputs are partitioned by "month" and written with dynamic partition
    # overwrite.  sort_by controls the physical sort order within each Parquet
    # row-group so downstream dashboards can exploit predicate push-down on the
    # most commonly filtered high-cardinality columns.
    write(
        top_articles_by_indegree(clean_df),
        "top_articles_indegree",
        partition_by=["month"],
        sort_by=["curr"],            # sort by target article (high cardinality)
    )
    write(
        top_articles_by_outdegree(clean_df),
        "top_articles_outdegree",
        partition_by=["month"],
        sort_by=["prev"],            # sort by source article (high cardinality)
    )
    write(
        top_link_pairs(clean_df),
        "top_link_pairs",
        partition_by=["month"],
        sort_by=["prev", "curr"],    # source → target traversal order
    )
    write(
        monthly_trend(clean_df),
        "monthly_trend",
        partition_by=["month"],
        sort_by=["curr"],
    )

    # ── Preview ────────────────────────────────────────────────────────────────
    # Read from already-written Parquet to avoid re-scanning 200M+ rows.
    print("\n── Top 10 articles by inbound clicks (latest month) ──")
    latest = months_loaded[-1]
    spark.read.parquet(
        str(Path(config.BATCH_OUTPUT_DIR).resolve() / "top_articles_indegree")
    ).filter(F.col("month") == latest).show(10, truncate=False)

    print("\n── Top 10 internal link pairs (latest month) ──")
    spark.read.parquet(
        str(Path(config.BATCH_OUTPUT_DIR).resolve() / "top_link_pairs")
    ).filter(F.col("month") == latest).show(10, truncate=False)

    spark.stop()
    print("Batch job complete.")


if __name__ == "__main__":
    main()
