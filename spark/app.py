# spark/app.py
import os, time, pathlib
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, expr, count, when,
    session_window, approx_count_distinct, coalesce, lit
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# ------------ Env ------------
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "ClickstreamSparkApp")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "events")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/opt/spark-output/parquet")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", os.path.join(OUTPUT_PATH, "_chk"))
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "10 seconds")
SESSION_WINDOW = os.getenv("SESSION_WINDOW", "30 minutes")
PAGE_WINDOW = os.getenv("PAGE_WINDOW", "20 seconds")
GEO_WINDOW = os.getenv("GEO_WINDOW", "30 seconds")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "30 seconds")
RUN_ID = os.getenv("RUN_ID", str(int(time.time())))

# ensure mount roots exist (no-op if present)
for p in [OUTPUT_PATH, CHECKPOINT_PATH]:
    pathlib.Path(p).mkdir(parents=True, exist_ok=True)

# ------------ Spark session ------------
spark = (
    SparkSession.builder
    .appName(SPARK_APP_NAME)
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

# ------------ Schema & input ------------
click_schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("page", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("device", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("country", StringType(), True),
    StructField("campaign", StringType(), True),
    StructField("latency_ms", LongType(), True),
    StructField("revenue", DoubleType(), True)
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", KAFKA_STARTING_OFFSETS)
    .option("failOnDataLoss", "false")
    .option("minPartitions", "3")
    .option("maxOffsetsPerTrigger", "3000")
    .load()
)

# Debug taps
(raw.selectExpr("CAST(value AS STRING) AS v", "timestamp")
    .writeStream.format("console")
    .option("truncate", False).option("numRows", 10)
    .outputMode("append").start())

parsed = (
    raw.selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_timestamp")
       .select(from_json(col("json_str"), click_schema).alias("j"), col("kafka_timestamp"))
       .select("j.*", "kafka_timestamp")
)

parsed = parsed.withColumn(
    "event_time_ts",
    to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX][X]")
)

events = (
    parsed
    .withColumn("event_ts", coalesce(col("event_time_ts"), col("kafka_timestamp")))
    .withColumn("event_type", coalesce(col("event_type"), lit("page_view")))
    .withWatermark("event_ts", WATERMARK_DELAY)
)

(events.select("event_ts","user_id","event_type","page","country","browser","campaign","revenue")
    .writeStream.format("console").option("truncate", False).option("numRows", 20)
    .outputMode("append").start())

# ------------ Aggregations ------------
page_metrics = (
    events.groupBy(window(col("event_ts"), PAGE_WINDOW).alias("w"), col("page"))
    .agg(
        count(lit(1)).alias("pageviews"),
        approx_count_distinct("user_id").alias("unique_users"),
        approx_count_distinct("session_id").alias("unique_sessions"),
        count(when(col("event_type") == "purchase", True)).alias("purchases"),
        expr("sum(coalesce(revenue,0.0))").alias("revenue")
    )
    .select(
        col("w.start").alias("window_start"),
        col("w.end").alias("window_end"),
        "page", "pageviews", "unique_users", "unique_sessions", "purchases", "revenue"
    )
)

geo_metrics = (
    events.groupBy(window(col("event_ts"), GEO_WINDOW), col("country"), col("browser"))
    .agg(count(lit(1)).alias("events"))
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "country", "browser", "events"
    )
)

sessions = (
    events.filter(col("user_id").isNotNull())
    .groupBy(col("user_id"), session_window(col("event_ts"), SESSION_WINDOW).alias("swin"))
    .agg(
        count(lit(1)).alias("events_in_session"),
        approx_count_distinct("page").alias("distinct_pages"),
        expr("sum(coalesce(revenue,0.0))").alias("session_revenue")
    )
    .select(
        "user_id",
        col("swin.start").alias("session_start"),
        col("swin.end").alias("session_end"),
        "events_in_session", "distinct_pages", "session_revenue"
    )
)

# ------------ Sinks (Parquet + CSV) ------------
checkpoint_base = os.path.join(CHECKPOINT_PATH, RUN_ID)

def write_streams(df, name, partitions=1):
    """
    Write df to both parquet and csv, but only when the micro-batch has rows.
    Also coalesce to limit the number of files and set a max rows per file.
    """
    base = os.path.join(OUTPUT_PATH, name)
    parquet_dir = os.path.join(base, "parquet")
    csv_dir = os.path.join(base, "csv")
    for p in [parquet_dir, csv_dir]:
        pathlib.Path(p).mkdir(parents=True, exist_ok=True)

    chk = os.path.join(checkpoint_base, name)

    def _writer(batch_df, batch_id: int):
        # Fast empty-batch check (prevents empty files)
        if batch_df.rdd.isEmpty():
            return
        out = batch_df.coalesce(partitions).persist()   # avoid recompute for two writes
        # Parquet
        (out.write
            .mode("append")
            .option("maxRecordsPerFile", 50000)         # limit small files
            .parquet(parquet_dir))
        # CSV
        (out.write
            .mode("append")
            .option("header", True)
            .option("maxRecordsPerFile", 50000)
            .csv(csv_dir))
        out.unpersist()

    (df.writeStream
        .foreachBatch(_writer)
        .option("checkpointLocation", chk)
        .outputMode("append")                           # append is still correct for file sinks
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start())
    print(f"[streams] Started {name} -> parquet & csv via foreachBatch", flush=True)

# Optional raw writer (dev) — proves file writes without waiting for windows
(events.coalesce(1).writeStream
    .format("parquet")
    .option("checkpointLocation", os.path.join(CHECKPOINT_PATH, "_chk_events"))
    .option("path", os.path.join(OUTPUT_PATH, "events_raw", "parquet"))
    .outputMode("append")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start())
print("[streams] Started events_raw -> parquet", flush=True)

write_streams(page_metrics, "page_metrics", partitions=1)
write_streams(geo_metrics,  "geo_metrics",  partitions=1)
write_streams(sessions,     "sessions",     partitions=1)

print("[streams] Awaiting termination...", flush=True)
spark.streams.awaitAnyTermination()
