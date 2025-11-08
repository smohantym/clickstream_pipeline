# 1) docker-compose.yml — the mini data platform

You spin up five services:

**zookeeper**

* Coordinates Kafka brokers.
* Healthcheck makes sure ZK is really responding before Kafka starts.

**kafka**

* Single-broker dev cluster.
* Dual listeners:

  * `PLAINTEXT://kafka:9092` (internal Docker network, used by producer & Spark)
  * `PLAINTEXT_HOST://localhost:9094` (optional host access, e.g., CLI from your laptop)
* Persists logs in a Docker volume (`kafka-data`).
* Healthcheck: waits until the broker can answer admin RPCs.

**kafka-init**

* One-shot container that creates the topic if missing (`KAFKA_TOPIC`, partitions, replication).
* Loops until Kafka is really ready; then runs `kafka-topics --create --if-not-exists`.

**spark-master**

* Spark Standalone master (`spark://spark-master:7077`).
* Exposes web UI on `8080`.
* Mounts `./data/output` → `/opt/spark-output` so Spark jobs can write to your host.

**spark-worker**

* Registers with the master and provides **2 cores / 2 GiB** (via `.env`).
* Web UI on `8081`.
* Shares the same `/opt/spark-output` mount so executors can write files the driver can see.

**spark-streaming**

* Submits your Structured Streaming app (`spark/app.py`) to the master.
* Installs the Kafka connector with `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1`.
* Prepares output/ivy dirs and exports a fresh `RUN_ID` per container start (so checkpoints are segregated).
* All knobs (windows, watermark, offsets, trigger interval, etc.) are passed in via env.

**producer**

* Lightweight Python container that generates fake clickstream events to Kafka.
* Depends on Kafka & topic being ready.

Why this layout?

* It’s the smallest realistic “platform”: a message bus (Kafka) + compute (Spark) + storage (Parquet/CSV on a shared mount), wired with health-gated startup so services come alive in the right order.

---

# 2) .env — all the knobs in one place

* **Kafka**: IDs, listeners, advertised listeners, partitions for your topic.
* **Producer**: `MSGS_PER_SEC`, `MSG_ACKS`, `MSG_COMPRESSION`.
* **Spark**: cluster URL, shuffle partitions, worker cores/memory, log level.
* **Streaming**: `KAFKA_STARTING_OFFSETS`, **short dev windows** (20–30s) and **short watermark** (10s) so you see output quickly; **trigger interval** (10s) to control batch cadence.
* **Paths**: `OUTPUT_PATH` and `CHECKPOINT_PATH` inside containers, both mapped to `./data/output` on your host.

Why env?

* Makes the whole system configurable without editing code; easy to swap dev vs “prod-ish” settings.

---

# 3) producer/ — synthetic clickstream generator

**producer.py**

* Chooses realistic fields (page, referrer, device, country, campaign).
* Randomly picks an `event_type` with skew (mostly `page_view`, some `click/add_to_cart`, rare `purchase` with `revenue`).
* **KafkaProducer settings**:

  * `acks=1`: leader ack → good dev balance of reliability/latency.
  * `linger_ms=0`, small `batch_size`: push events promptly so streaming picks them up quickly.
  * Retries, timeouts, and `max_in_flight_requests_per_connection=1` for simple, predictable behavior.
* **wait_for_topic** polls metadata until the topic is definitely visible (avoids “send before topic exists” race).
* Sends at ~`MSGS_PER_SEC`, flushes/awaits ack every 10th message to exercise the happy path and surface errors early.

**requirements.txt**

* `kafka-python` client + `faker` (you’re using `random` here; Faker’s available if you want more realism).

Why this producer?

* It’s fast to start, low-overhead, and stable for demos. The periodic ack & flush give you confidence data is flowing.

---

# 4) spark/app.py — end-to-end stream processing

## 4.1 Spark session & config

* `spark.sql.adaptive.enabled=false`: avoid AQE plan changes mid-stream (simplifies dev).
* `spark.sql.streaming.forceDeleteTempCheckpointLocation=true`: silences temp checkpoint warnings when you **also** set a real checkpoint.
* `spark.sql.session.timeZone=UTC`: consistent cross-container timestamps.
* Log level from env (default `WARN`).

## 4.2 Reading Kafka

```python
spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
  .option("subscribe", KAFKA_TOPIC) \
  .option("startingOffsets", KAFKA_STARTING_OFFSETS) \
  .option("failOnDataLoss", "false") \
  .option("minPartitions", "3") \
  .option("maxOffsetsPerTrigger", "3000") \
  .load()
```

* `startingOffsets`: `latest` for dev (only new data), `earliest` if you want to backfill.
* `failOnDataLoss=false`: robust to log truncation/retention in dev.
* `minPartitions=3`: try to parallelize to match Kafka partitions.
* `maxOffsetsPerTrigger=3000`: throttle per micro-batch (prevents huge spikes).

Two **console taps**:

* Raw `value` + Kafka `timestamp` → sanity check that the consumer is alive.
* Post-parse selected columns → verify parsing and schema.

## 4.3 Parsing & event time

* JSON → struct using an explicit schema.
* `event_time` parsed to timestamp (`to_timestamp` with flexible pattern).
* `event_ts = coalesce(event_time_ts, kafka_timestamp)`: if payload time is missing/bad, fall back to Kafka ingestion time → your windows still work.

## 4.4 Watermark & windows

* `.withWatermark("event_ts", WATERMARK_DELAY)` tells Spark how long to wait for late events before finalizing windowed aggregations.
* Dev values are short (10s, 20–30s windows) so you see files quickly. In production, use minutes.

## 4.5 Aggregations

**page_metrics** (tumbling window by `PAGE_WINDOW`, grouped by `page`)

* `count(*)`, `approx_count_distinct` for users/sessions, purchase count, revenue sum.

**geo_metrics** (tumbling by `GEO_WINDOW`)

* `events` by `(country, browser)`.

**sessions** (sessionization)

* `session_window(event_ts, SESSION_WINDOW)` groups sparse clicks for each user.
* Outputs per user session: `events_in_session`, distinct `page` count, and `session_revenue`.

Why `approx_count_distinct`?

* HyperLogLog-style cardinality—lightweight for streaming and scalable.

## 4.6 Sinks — robust file writing with foreachBatch

```python
def write_streams(df, name, partitions=1):
    def _writer(batch_df, batch_id):
        if batch_df.rdd.isEmpty():  # skip empty batches → no empty files
            return
        out = batch_df.coalesce(partitions).persist()  # fewer files & reuse for two writes
        (out.write.mode("append").option("maxRecordsPerFile", 50000).parquet(parquet_dir))
        (out.write.mode("append").option("header", True)
             .option("maxRecordsPerFile", 50000).csv(csv_dir))
        out.unpersist()

    (df.writeStream
        .foreachBatch(_writer)              # single pass → write multiple sinks
        .option("checkpointLocation", chk)  # REQUIRED for exactly-once semantics
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start())
```

* **Why `foreachBatch`?** You write **Parquet and CSV in one micro-batch** without re-computing, and you can **skip empty batches**. This avoids empty CSV headers and reduces small files.
* `coalesce(partitions)`: controls parallelism of file output. With `1`, you’ll get at most one file per sink per batch (nice for demos).
* `maxRecordsPerFile`: caps file size to prevent monster files in long runs.
* **Checkpoints**: `checkpoint_base = CHECKPOINT_PATH/RUN_ID` isolates runs. You also use a dedicated `_chk_events` for the raw stream.

**Optional raw writer**

* Writes every event row to Parquet, coalesced to one file per batch—useful to prove IO even before windows finish.

Directory layout on host (mounted from `/opt/spark-output/parquet`):

```
./data/output/parquet/
  events_raw/parquet/...
  page_metrics/{parquet,csv}/...
  geo_metrics/{parquet,csv}/...
  sessions/{parquet,csv}/...
  _chk/         (global checkpoints)
```

> If you later want **date partitioning**, add inside `_writer`:
>
> ```python
> from pyspark.sql.functions import to_date
> out = out.withColumn("dt", to_date(col("event_ts")))
> (out.write.mode("append").partitionBy("dt")....parquet(parquet_dir))
> (out.write.mode("append").partitionBy("dt").option("header", True)....csv(csv_dir))
> ```

---

# 5) End-to-end workflow (what happens when you run it)

1. **Compose up**
   `docker compose up -d --build`

   * ZK starts → Kafka starts & passes healthcheck → `kafka-init` creates `events` topic.
   * Spark master comes up; worker registers.
   * `spark-streaming` submits the job to master, installs Kafka connector (cached in `/tmp/.ivy2`).
   * Producer connects to Kafka, confirms topic partitions, and starts pushing ~5 msg/s.

2. **Data flow**

   * Producer → Kafka (`events` topic).
   * Spark Structured Streaming reads from Kafka in micro-batches (every `TRIGGER_INTERVAL`, e.g., 10s), respecting `maxOffsetsPerTrigger`.
   * Spark parses JSON, derives `event_ts`, applies watermark & windows.
   * Each micro-batch:

     * Debug rows print to console (so you can see activity).
     * Aggregations produce rows once a window is ready (after watermark).
     * `foreachBatch` writes **both Parquet and CSV** for non-empty results, with **coalesced output** and **maxRecordsPerFile**.

3. **Where to look**

   * Spark master UI: `http://localhost:8080` → see app, executors, stages.
   * Spark worker UI: `http://localhost:8081` → confirm resources/executor launched.
   * `docker logs spark-streaming` → console sinks and “[streams] Started …” messages.
   * Files on your host: `./data/output/parquet/**`.

---

# 6) Operating & Tuning

**Common quick checks**

* Producer is running: `docker logs -f producer` (look for `[ack] topic:partition@offset`).
* Kafka topic exists: `docker exec -it kafka bash -lc "/usr/bin/kafka-topics --bootstrap-server kafka:9092 --list | grep -w events"`.
* Spark app submitted & executor launched: master/worker logs; master UI app list.
* Files appear under `./data/output/parquet/...` after a couple of triggers.

**Throughput knobs**

* Producer: `MSGS_PER_SEC`, compression (e.g., `MSG_COMPRESSION=gzip`).
* Spark read: `maxOffsetsPerTrigger`, `minPartitions`.
* Shuffle: `SPARK_SQL_SHUFFLE_PARTITIONS` (raise for heavier joins/aggs).
* Output: increase `partitions` in `write_streams(df, name, partitions=…)` if you want parallel file writes.

**Window/watermark behavior**

* With **short dev windows (20–30s)** and **watermark=10s**, expect files ~every 1–2 trigger intervals after the window boundary. Larger production windows will delay outputs appropriately but give more stable aggregates.

**File explosion control**

* You already coalesce per sink and set `maxRecordsPerFile`.
* For multi-hour runs or larger volumes, add **date partitioning** (`partitionBy("dt")`) and maybe compact older data with a batch job.

**Restarting cleanly**

* Because you use `RUN_ID` per container start, each run writes to a fresh checkpoint tree—this avoids “old checkpoint state” surprises during dev.
* If you want **true exactly-once** across restarts for the same output, set a **stable** checkpoint path (remove `RUN_ID`) once you’re confident.