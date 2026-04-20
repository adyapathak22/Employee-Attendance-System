"""
spark/spark_streaming.py
TASK 21 — Structured Streaming: Spark streaming job for real-time attendance logs
"""
import os
import json
import logging
from pathlib import Path
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, TimestampType
    )
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False


EVENT_SCHEMA = StructType([
    StructField("event_id",     StringType(),    True),
    StructField("employee_id",  StringType(),    True),
    StructField("event_type",   StringType(),    True),
    StructField("timestamp",    StringType(),    True),
    StructField("department",   StringType(),    True),
    StructField("location",     StringType(),    True),
    StructField("device_id",    StringType(),    True),
    StructField("source",       StringType(),    True),
    StructField("latitude",     DoubleType(),    True),
    StructField("longitude",    DoubleType(),    True),
])


def run_file_streaming(spark):
    """
    Spark Structured Streaming — reads from a file source (simulates Kafka).
    In production replace readStream source with 'kafka'.
    """
    input_dir = "data/streaming/input"
    checkpoint_dir = "data/streaming/checkpoint"
    output_dir = "data/streaming/output"
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(checkpoint_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    # Write some test events to input dir so streaming has data
    _seed_input_files(input_dir, n=5)

    log.info("Starting Spark Structured Streaming (file source)...")

    # ── Read Stream ────────────────────────────────────────────
    stream_df = (
        spark.readStream
        .schema(EVENT_SCHEMA)
        .option("maxFilesPerTrigger", 1)
        .json(input_dir)
    )

    # ── Transformations ────────────────────────────────────────
    enriched = (
        stream_df
        .withColumn("event_ts", F.to_timestamp("timestamp"))
        .withColumn("hour",     F.hour("event_ts"))
        .withColumn("is_checkin", (F.col("event_type") == "CHECK_IN").cast("int"))
        .withColumn("is_late",
            (F.col("is_checkin") == 1) & (F.col("hour") >= 10)
        )
        .withColumn("processed_at", F.current_timestamp())
    )

    # ── Aggregation with Watermark (handles late data) ────────
    windowed_counts = (
        enriched
        .withWatermark("event_ts", "10 minutes")
        .groupBy(
            F.window("event_ts", "5 minutes"),
            "department",
            "event_type"
        )
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("employee_id").alias("unique_employees")
        )
    )

    # ── Write Stream: Complete mode aggregations ───────────────
    query_agg = (
        windowed_counts
        .writeStream
        .outputMode("complete")
        .format("memory")
        .queryName("attendance_stream_agg")
        .option("checkpointLocation", checkpoint_dir + "/agg")
        .trigger(processingTime="5 seconds")
        .start()
    )

    # ── Write Stream: Append raw events to parquet ────────────
    query_raw = (
        enriched
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", output_dir)
        .option("checkpointLocation", checkpoint_dir + "/raw")
        .trigger(processingTime="5 seconds")
        .start()
    )

    log.info("Streaming queries started. Running for 20 seconds...")
    import time
    time.sleep(20)

    # Show live aggregation results
    log.info("Live aggregation results:")
    spark.sql("SELECT * FROM attendance_stream_agg ORDER BY window DESC LIMIT 20").show(truncate=False)

    query_agg.stop()
    query_raw.stop()
    log.info("Streaming queries stopped.")


def run_kafka_streaming(spark):
    """
    Kafka-based Spark Structured Streaming.
    Requires Kafka running on localhost:9092.
    """
    log.info("Kafka Structured Streaming (requires Kafka running)...")
    try:
        raw_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "attendance-events")
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        parsed = (
            raw_stream
            .select(
                F.col("key").cast("string").alias("employee_id"),
                F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("data"),
                F.col("timestamp").alias("kafka_ts"),
                F.col("partition"),
                F.col("offset"),
            )
            .select("employee_id", "data.*", "kafka_ts", "partition", "offset")
            .withColumn("event_ts", F.to_timestamp("timestamp"))
        )

        # Real-time attendance count per department (tumbling 1-min window)
        dept_counts = (
            parsed
            .withWatermark("event_ts", "2 minutes")
            .groupBy(F.window("event_ts", "1 minute"), "department")
            .agg(F.count("*").alias("events"))
        )

        query = (
            dept_counts
            .writeStream
            .outputMode("update")
            .format("console")
            .option("truncate", "false")
            .trigger(processingTime="10 seconds")
            .start()
        )

        query.awaitTermination(60)
        query.stop()
    except Exception as e:
        log.warning(f"Kafka streaming failed: {e}. Use file streaming instead.")


def _seed_input_files(input_dir: str, n: int = 5):
    """Create sample JSON event files for file-source streaming."""
    import random
    depts = ["Engineering", "HR", "Finance", "Sales"]
    events = ["CHECK_IN", "CHECK_OUT", "OVERTIME_START"]
    for i in range(n):
        records = []
        for j in range(20):
            records.append({
                "event_id": f"EVT{i*100+j:06d}",
                "employee_id": f"EMP{random.randint(1,200):04d}",
                "event_type": random.choice(events),
                "timestamp": datetime.utcnow().isoformat(),
                "department": random.choice(depts),
                "location": "Floor-1",
                "device_id": f"DEVICE{random.randint(1,20):03d}",
                "source": "biometric",
                "latitude": 26.8467,
                "longitude": 80.9462,
            })
        fname = f"{input_dir}/events_batch_{i:03d}.json"
        with open(fname, "w") as f:
            for r in records:
                f.write(json.dumps(r) + "\n")
    log.info(f"  Seeded {n} JSON batch files into {input_dir}")


def main():
    if not SPARK_AVAILABLE:
        log.error("PySpark not installed. Run: pip install pyspark")
        return

    spark = (
        SparkSession.builder
        .appName("AttendanceStreaming")
        .master("local[2]")
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    log.info("=== Task 21: Spark Structured Streaming ===\n")
    run_file_streaming(spark)

    spark.stop()
    log.info("✅ Task 21 Complete!")


if __name__ == "__main__":
    main()
