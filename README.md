# Wikipedia Clickstream Pipeline

A **Lambda-architecture** demo built on two free Wikimedia datasets:

| Layer | Dataset | Transport |
|-------|---------|-----------|
| Batch | Monthly clickstream TSV dumps | Direct HTTP download |
| Streaming | EventStreams recentchange feed | SSE → Kafka → Spark |

**Cost: $0** — both data sources are free. You only pay for compute.

---

## Project layout

```
wiki_clickstream_project/
├── config.py                      # All tuneable settings
├── requirements.txt
├── docker/
│   └── docker-compose.yml         # Single-node Kafka (KRaft) + Kafka UI
├── ingestion/
│   ├── download_clickstream.py    # Download monthly TSV.GZ dumps
│   └── sse_to_kafka.py            # Tail EventStreams → publish to Kafka
├── batch/
│   └── clickstream_batch.py       # PySpark batch analysis job
├── streaming/
│   └── kafka_to_spark.py          # Spark Structured Streaming job
└── data/                          # Created at runtime (git-ignored)
    ├── raw/                       # Downloaded TSV.GZ files
    ├── batch_output/              # Parquet from batch job
    ├── streaming_output/          # Parquet from streaming job
    └── streaming_checkpoint/      # Spark checkpoint dirs
```

---

## Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Python | ≥ 3.10 | |
| Java | 11 or 17 | Required by Spark |
| Apache Spark | ≥ 3.5 | `SPARK_HOME` must be on `PATH` |
| Docker Desktop | any recent | For the local Kafka stack |

Install Python dependencies:

```bash
pip install -r requirements.txt
```

---

## Quickstart

### Step 1 — Start Kafka

```bash
cd docker
docker compose up -d
# Kafka UI available at http://localhost:8080
```

Wait ~30 s for the health-check to pass, then continue.

### Step 2 — Download clickstream batch data

Edit `config.py` to choose which months you want (default: 6 months of enwiki).

```bash
python ingestion/download_clickstream.py
```

Files land in `data/raw/`. Each English Wikipedia month is ~200–400 MB gzipped.

### Step 3 — Run the batch Spark job

```bash
python batch/clickstream_batch.py
```

Parquet output is written to `data/batch_output/`:

| Folder | Contents |
|--------|----------|
| `top_articles_indegree/` | Top-50 most-clicked-to articles |
| `top_articles_outdegree/` | Top-50 articles sending most clicks |
| `top_link_pairs/` | Top-50 prev→curr link pairs |
| `monthly_trend/` | Partitioned by month; click volume per article |

### Step 4 — Start the SSE → Kafka producer

```bash
python ingestion/sse_to_kafka.py
```

Tails `https://stream.wikimedia.org/v2/stream/recentchange` and publishes
every enwiki edit to the `wiki-edits` Kafka topic. Reconnects automatically.

### Step 5 — Run the Spark Structured Streaming job

In a **separate terminal**:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  streaming/kafka_to_spark.py
```

Or without `spark-submit` (Spark downloads the JAR automatically):

```bash
python streaming/kafka_to_spark.py
```

Streaming outputs written to `data/streaming_output/`:

| Folder | Contents |
|--------|----------|
| `edits_per_wiki_per_minute/` | Edit rate + bot ratio, 1-min tumbling window |
| `top_editors_sliding/` | Top human editors, 5-min sliding window |
| `edit_type_breakdown/` | new / edit / categorize counts, 1-min window |

A live console preview refreshes every 30 seconds.

---

## Configuration reference (`config.py`)

| Setting | Default | Description |
|---------|---------|-------------|
| `CLICKSTREAM_MONTHS` | 6 months | List of `"YYYY-MM"` strings to download |
| `CLICKSTREAM_WIKI` | `"enwiki"` | Which wiki to pull |
| `RAW_DATA_DIR` | `data/raw` | Where to save `.tsv.gz` files |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka address |
| `KAFKA_TOPIC_EDITS` | `wiki-edits` | Kafka topic name |
| `EVENTSTREAMS_WIKI_FILTER` | `"enwiki"` | Filter SSE to one wiki; `None` = all |
| `SPARK_MASTER` | `local[*]` | Change to `yarn` / `spark://…` for cluster |
| `SPARK_KAFKA_PACKAGE` | `…3.5.0` | Must match your Spark + Scala versions |

---

## Architecture diagram

```
┌─────────────────────────────────────────┐
│           BATCH LAYER                   │
│                                         │
│  dumps.wikimedia.org  ──HTTP──►  data/  │
│  (monthly TSV.GZ)               raw/   │
│                                   │     │
│                             PySpark job │
│                                   │     │
│                          data/batch_output/ (Parquet)
└─────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│           STREAMING LAYER               │
│                                         │
│  stream.wikimedia.org/v2/stream/        │
│  recentchange (SSE)                     │
│       │                                 │
│  sse_to_kafka.py                        │
│       │                                 │
│  Kafka topic: wiki-edits                │
│       │                                 │
│  Spark Structured Streaming             │
│  (windowed aggregations)                │
│       │                                 │
│  data/streaming_output/ (Parquet)       │
└─────────────────────────────────────────┘
```

---

## Extending the project

- **Serve results** — expose `data/batch_output/` via a Flask API or connect
  Power BI / Superset directly to the Parquet files.
- **Kappa architecture** — replay the TSV dump files through Kafka using a
  simple file-replay producer and unify both layers through the same streaming
  pipeline.
- **Cloud deployment** — swap `SPARK_MASTER` for a Databricks or EMR cluster
  endpoint; replace the local Kafka with Amazon MSK or Confluent Cloud.
- **Delta Lake** — replace plain Parquet sinks with Delta tables for ACID
  updates and time-travel queries.

---

## Data sources

| Source | URL | License |
|--------|-----|---------|
| Clickstream dumps | https://dumps.wikimedia.org/other/clickstream/ | CC0 |
| EventStreams | https://stream.wikimedia.org/v2/stream/recentchange | CC0 |
