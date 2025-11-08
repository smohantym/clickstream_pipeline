# Clickstream Pipeline (Kafka + Spark Structured Streaming)

A reproducible, containerised clickstream analytics pipeline for local development. It produces synthetic web events to Kafka, processes them with Spark Structured Streaming, and lands aggregated outputs to Parquet and CSV on your host machine.

## ✨ Features
- **Self-contained** via Docker Compose (Kafka, Zookeeper, Spark master/worker, producer, streaming job)
- **Synthetic data generator** with realistic event mix (page views, clicks, add-to-cart, purchases)
- **Exactly-once-ish** file outputs using Structured Streaming + checkpoints
- **Windowed aggregations** (page metrics, geo metrics) and **sessionisation** per user
- **Dual sinks** per stream (Parquet + CSV) written efficiently via `foreachBatch`
- **Dev-friendly defaults** (short windows/watermark, small batch sizes, simple scaling knobs)

---

## 🧱 Architecture
```
[Faker Producer] → Kafka (topic: events) → Spark Structured Streaming
                                │
                                ├── page_metrics  → Parquet + CSV
                                ├── geo_metrics   → Parquet + CSV
                                └── sessions      → Parquet + CSV

All outputs are written to: ./data/output/parquet (mounted as /opt/spark-output)
```

**Services**
- `zookeeper`: required by Kafka
- `kafka`: single-broker dev cluster (internal listener for containers, host listener for local tools)
- `kafka-init`: creates the topic if missing
- `spark-master`: Spark Standalone master (UI: :8080)
- `spark-worker`: executor resources (UI: :8081)
- `spark-streaming`: submits and runs `spark/app.py`
- `producer`: generates synthetic events continuously

---

## 📁 Project Structure
```
clickstream_pipeline/
├─ docker-compose.yml
├─ .env
├─ data/
│  └─ output/
│     └─ parquet/               # all outputs land here (mounted to /opt/spark-output)
├─ producer/
│  ├─ Dockerfile
│  ├─ requirements.txt
│  └─ producer.py
└─ spark/
   └─ app.py
```

---

## 🔧 Prerequisites
- Docker 24+ and Docker Compose V2
- ~4 GB free RAM recommended for smooth Spark execution
- Ports available: `2181`, `9092`, `9094`, `7077`, `8080`, `8081`

---

## ⚙️ Configuration (.env)
All key knobs live in **.env**:

- **Kafka**: listeners, topic name/partitions, replication factor
- **Producer**: `MSGS_PER_SEC`, `MSG_ACKS`, `MSG_COMPRESSION`
- **Spark cluster**: `SPARK_MASTER_URL`, worker cores/memory, shuffle partitions
- **Streaming**: `KAFKA_STARTING_OFFSETS`, `WATERMARK_DELAY`, `SESSION_WINDOW`, `PAGE_WINDOW`, `GEO_WINDOW`, `TRIGGER_INTERVAL`
- **Paths**: `OUTPUT_PATH` and `CHECKPOINT_PATH` inside containers (mounted to `./data/output`)

> Dev defaults are set for quick feedback (short windows & watermark). See the **Production-ish Settings** section for alternatives.

---

## 🚀 Quick Start
```bash
# 1) Build and launch
docker compose up -d --build

# 2) Check services
docker compose ps

# 3) Tail logs (optional)
docker logs -f spark-streaming
# and/or
docker logs -f producer

# 4) Inspect outputs on host
ls -R ./data/output/parquet
```

When healthy, you’ll see:
- Producer logs confirming acks every ~10 messages
- Spark streaming logs with console sinks printing sample rows
- Output folders filling under `./data/output/parquet/`:
  - `events_raw/parquet/`
  - `page_metrics/{parquet,csv}/`
  - `geo_metrics/{parquet,csv}/`
  - `sessions/{parquet,csv}/`

Stop everything:
```bash
docker compose down -v   # -v also removes volumes (Kafka data)
```

---

## 🔍 Verifying Data Flow
- **Kafka topic exists**
  ```bash
  docker exec -it kafka bash -lc \
    "/usr/bin/kafka-topics --bootstrap-server kafka:9092 --list | grep -w events"
  ```
- **Consume a few messages (from inside Kafka container)**
  ```bash
  docker exec -it kafka bash -lc \
    "/usr/bin/kafka-console-consumer --bootstrap-server kafka:9092 \
     --topic events --from-beginning --max-messages 5 --timeout-ms 3000"
  ```
- **Spark UIs**
  - Master UI: http://localhost:8080
  - Worker UI: http://localhost:8081

---

## 🧠 How It Works

### Producer (producer/producer.py)
- Generates realistic events (pages, referrers, device/browser mix).
- Uses `kafka-python` to send at ~`MSGS_PER_SEC`.
- Waits for topic metadata before sending (avoids races).
- Periodically awaits broker acks and flushes buffers → early error surfacing.

### Stream App (spark/app.py)
- **Read**: `spark.readStream.format("kafka")` from `KAFKA_BOOTSTRAP_SERVERS`, subscribes to `KAFKA_TOPIC`.
- **Parse**: JSON → typed columns via an explicit schema; derive `event_ts` from payload time with fallback to Kafka timestamp.
- **Watermark**: `.withWatermark("event_ts", WATERMARK_DELAY)` to bound state and finalize windows.
- **Aggregations**:
  - `page_metrics`: tumbling window by `PAGE_WINDOW`, grouped by `page` → pageviews, approx distinct users/sessions, purchases, revenue.
  - `geo_metrics`: tumbling window by `GEO_WINDOW` → events by `(country, browser)`.
  - `sessions`: per-user `session_window(event_ts, SESSION_WINDOW)` → events count, distinct pages, session revenue.
- **Sinks**: `foreachBatch` writes **Parquet and CSV** per micro-batch while skipping empty batches and limiting small files via `coalesce(partitions)` + `maxRecordsPerFile`.
- **Checkpoints**: one checkpoint dir per stream (`CHECKPOINT_PATH/<RUN_ID>/<stream_name>`) for reliable progress and recovery.

---

## ⚖️ Tuning & Scaling
- **More input**: increase `MSGS_PER_SEC` in `.env`.
- **Micro-batch size**: raise/lower `maxOffsetsPerTrigger`.
- **Parallelism**: adjust `SPARK_SQL_SHUFFLE_PARTITIONS` and `write_streams(..., partitions=…)`.
- **Windowing**: for production, prefer longer windows and watermarks (e.g., 1–5 minutes+) for more stable aggregates.
- **File size/Count**: increase `maxRecordsPerFile`; for long runs add partitioning by date inside `foreachBatch`:
  ```python
  from pyspark.sql.functions import to_date
  out = out.withColumn("dt", to_date(col("event_ts")))
  (out.write.mode("append").partitionBy("dt").parquet(parquet_dir))
  (out.write.mode("append").partitionBy("dt").option("header", True).csv(csv_dir))
  ```

---

## 🛠️ Troubleshooting
- **"Initial job has not accepted any resources"**
  - Ensure `spark-worker` is running and registered (check Master UI and worker logs).
  - Confirm worker has enough cores/memory as requested; tweak `.env` (`SPARK_WORKER_CORES`, `SPARK_WORKER_MEMORY`).

- **No files written**
  - Check producer logs for `[ack]` and that topic has messages.
  - Confirm console sinks in `spark-streaming` logs show data.
  - Remember windowed outputs appear only after the watermark’s delay elapses.

- **Permission / path errors**
  - Ensure `./data/output` exists on host; Compose mounts it to `/opt/spark-output`.
  - The entrypoint `mkdir -p` and `chmod -R 777` cover most dev cases.

- **Old checkpoints cause weirdness**
  - This project segregates checkpoints by `RUN_ID`. If you switch to a stable checkpoint path, delete old `_chk` when changing schemas.

---

## 🧪 Useful Commands
```bash
# Tail producer or streaming logs
docker logs -f producer
docker logs -f spark-streaming

# Inspect output directories
find ./data/output/parquet -maxdepth 3 -type d -print

# Reset the world (remove containers and Kafka data)
docker compose down -v && rm -rf ./data/output/*
```

---

## 🧭 Production-ish Settings (suggested)
```env
KAFKA_STARTING_OFFSETS=latest
WATERMARK_DELAY=5 minutes
SESSION_WINDOW=30 minutes
PAGE_WINDOW=1 minute
GEO_WINDOW=5 minutes
TRIGGER_INTERVAL=1 minute
SPARK_SQL_SHUFFLE_PARTITIONS=200         # depends on cluster size & data volume
```
- Use stable `CHECKPOINT_PATH` (without RUN_ID) for true stateful recovery.
- Consider partitioning outputs by date (see snippet above).
- Enable compression on files (e.g., Parquet is compressed by default; CSV could be gzip via downstream tooling).

---

## 🙋 FAQ
**Q: Why `foreachBatch` instead of two separate `writeStream`s?**  
A: It avoids recomputation, lets you skip empty batches, and write multiple sinks atomically per micro-batch.

**Q: Why `approx_count_distinct`?**  
A: It’s HyperLogLog-based—ideal for streaming and large cardinalities without heavy state.

**Q: Can I read the outputs with Pandas or Spark later?**  
A: Yes—just point to the Parquet folders (recommended for analytics). CSVs are provided for convenience.

**Q: How do I push more data?**  
A: Increase `MSGS_PER_SEC` in `.env`, or run multiple producers with the same topic.

---

Happy streaming! 🚀

