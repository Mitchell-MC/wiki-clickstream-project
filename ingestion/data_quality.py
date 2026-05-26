"""
ingestion/data_quality.py
─────────────────────────
Data quality contract / validation engine for the Wikipedia clickstream
Bronze layer.

Contract rules
──────────────
  1. prev  — non-null, non-empty string (the referrer article or special token)
  2. curr  — non-null, non-empty string (the target article title)
  3. type  — must be one of: "link", "external", "other"
  4. n     — must be a positive integer (> 0); negative counts are impossible
  5. month — must match the YYYY-MM pattern derived during ingestion

Records that violate any constraint are quarantined to
QUARANTINE_DIR/clickstream/ partitioned by month and removed from the
returned clean DataFrame before it reaches any analytics layer.

Because every operation is expressed as a PySpark column expression, the
validation pass is fully vectorised — no Python-level row iteration.

Usage (called from batch/clickstream_batch.py):
    from ingestion.data_quality import validate_and_quarantine
    clean_df, n_bad = validate_and_quarantine(raw_df)
"""

import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config

# ── Contract constants ─────────────────────────────────────────────────────────

VALID_TYPES: frozenset[str] = frozenset({"link", "external", "other"})

# Pattern that every month column value must satisfy
_MONTH_RE = r"^\d{4}-\d{2}$"

# Internal flag-column names (dropped before returning)
_FLAG_COLS = [
    "_viol_null_prev",
    "_viol_null_curr",
    "_viol_bad_type",
    "_viol_neg_n",
    "_viol_bad_month",
    "_any_violation",
    "_reason",
]


# ── Private helpers ─────────────────────────────────────────────────────────────

def _add_violation_flags(df: DataFrame) -> DataFrame:
    """
    Append a boolean flag column per contract rule plus a summary
    _any_violation column.  All operations are column-level (vectorised).
    """
    return (
        df
        .withColumn(
            "_viol_null_prev",
            F.col("prev").isNull() | (F.trim(F.col("prev")) == ""),
        )
        .withColumn(
            "_viol_null_curr",
            F.col("curr").isNull() | (F.trim(F.col("curr")) == ""),
        )
        .withColumn(
            "_viol_bad_type",
            ~F.col("type").isin(*VALID_TYPES),
        )
        .withColumn(
            "_viol_neg_n",
            F.col("n") <= 0,
        )
        .withColumn(
            "_viol_bad_month",
            F.col("month").isNull() | ~F.col("month").rlike(_MONTH_RE),
        )
        .withColumn(
            "_any_violation",
            F.col("_viol_null_prev")
            | F.col("_viol_null_curr")
            | F.col("_viol_bad_type")
            | F.col("_viol_neg_n")
            | F.col("_viol_bad_month"),
        )
    )


def _add_reason_string(df: DataFrame) -> DataFrame:
    """
    Derive a pipe-delimited human-readable reason for each violated record.
    Example: "null_curr|neg_n"
    """
    return df.withColumn(
        "_reason",
        F.concat_ws(
            "|",
            F.when(F.col("_viol_null_prev"), F.lit("null_prev")),
            F.when(F.col("_viol_null_curr"), F.lit("null_curr")),
            F.when(F.col("_viol_bad_type"),  F.lit("bad_type")),
            F.when(F.col("_viol_neg_n"),     F.lit("neg_n")),
            F.when(F.col("_viol_bad_month"), F.lit("bad_month")),
        ),
    )


# ── Public interface ────────────────────────────────────────────────────────────

def validate_and_quarantine(df: DataFrame) -> tuple[DataFrame, int]:
    """
    Validate *df* against the clickstream data contract.

    Bad records are isolated, enriched with a _reason string, and written
    to QUARANTINE_DIR/clickstream/ partitioned by month using dynamic
    partition overwrite — a re-run for the same month safely overwrites
    only that month's quarantine partition.

    Parameters
    ----------
    df : Raw clickstream DataFrame.
         Required columns: prev, curr, type, n, month.

    Returns
    -------
    (clean_df, quarantine_count)
        clean_df         : DataFrame with all violating rows removed.
        quarantine_count : Number of quarantined records (0 = fully clean).
    """
    flagged = _add_violation_flags(df)
    flagged = _add_reason_string(flagged)

    bad  = flagged.filter( F.col("_any_violation"))
    good = flagged.filter(~F.col("_any_violation"))

    # Count violations first — cheap because bad rows are typically rare
    quarantine_count: int = bad.count()

    if quarantine_count > 0:
        print(
            f"[quality]  {quarantine_count:,} records failed data contract "
            f"— writing to quarantine …"
        )

        # Quarantine path uses the same dynamic-partition-overwrite semantics
        # as the main batch output: re-running a month replaces only that
        # month's quarantine partition, never touching other months.
        quarantine_root = (
            Path(config.QUARANTINE_DIR).resolve() / "clickstream"
        )
        quarantine_root.mkdir(parents=True, exist_ok=True)

        (
            bad
            # Retain _reason so analysts can triage failures; drop internal
            # per-rule flags since _reason already summarises them.
            .drop(
                "_viol_null_prev",
                "_viol_null_curr",
                "_viol_bad_type",
                "_viol_neg_n",
                "_viol_bad_month",
                "_any_violation",
            )
            .write
            .mode("overwrite")
            .partitionBy("month")
            .parquet(str(quarantine_root))
        )
        print(f"[quality]  Quarantine written → {quarantine_root}")
    else:
        print("[quality]  All records passed data contract validation.")

    # Strip all internal columns before returning the clean dataset
    clean_df = good.drop(*_FLAG_COLS)
    return clean_df, quarantine_count


def quarantine_summary(spark, month: str | None = None) -> None:
    """
    Print a breakdown of quarantine violations.

    Parameters
    ----------
    spark  : Active SparkSession.
    month  : If given, filter the report to a single YYYY-MM month.
             If None, report across all quarantined months.
    """
    quarantine_root = str(
        Path(config.QUARANTINE_DIR).resolve() / "clickstream"
    )
    try:
        q_df = spark.read.parquet(quarantine_root)
    except Exception:
        print("[quality]  No quarantine data found.")
        return

    if month:
        q_df = q_df.filter(F.col("month") == month)

    print(f"\n── Quarantine summary {'(month=' + month + ')' if month else '(all months)'} ──")
    (
        q_df
        .groupBy("month", "_reason")
        .count()
        .orderBy("month", F.desc("count"))
        .show(50, truncate=False)
    )
