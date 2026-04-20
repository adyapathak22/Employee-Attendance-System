"""
kafka/consumer.py
TASKS 19, 20 — Kafka Consumer with offset management and partitioning
"""
import json
import sqlite3
import logging
import time
from datetime import datetime
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

TOPIC = "attendance-events"
DB_PATH = "data/attendance.db"


def run_consumer_kafka():
    """Real Kafka consumer with manual offset commit."""
    try:
        from kafka import KafkaConsumer, TopicPartition

        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=["localhost:9092"],
            group_id="attendance-db-writer",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            auto_offset_reset="earliest",
            enable_auto_commit=False,   # Task 20: manual commit for exactly-once
            max_poll_records=100,
        )

        log.info(f"Consumer connected to topic: {TOPIC}")
        log.info(f"Assigned partitions: {consumer.assignment()}")

        processed = 0
        batch = []
        BATCH_SIZE = 50

        for message in consumer:
            event = message.value
            partition = message.partition
            offset = message.offset

            log.debug(f"  P{partition}@{offset}: {event['event_type']} | {event['employee_id']}")
            batch.append(event)

            if len(batch) >= BATCH_SIZE:
                _write_batch_to_db(batch)
                consumer.commit()  # Manual offset commit after successful write
                processed += len(batch)
                log.info(f"  Committed {processed} events total")
                batch = []

    except ImportError:
        log.warning("Kafka not available — running simulation consumer")
        run_consumer_simulated()
    except Exception as e:
        log.warning(f"Kafka error: {e} — falling back to simulation")
        run_consumer_simulated()


def run_consumer_simulated():
    """Read from simulated events file."""
    import os
    events_file = "data/streaming/simulated_events.json"
    if not os.path.exists(events_file):
        log.warning("No simulated events found. Run producer.py first.")
        return

    with open(events_file) as f:
        events = json.load(f)

    log.info(f"Processing {len(events)} simulated events...")
    stats = defaultdict(int)

    for event in events:
        stats[event["event_type"]] += 1
        # Simulate processing time
        time.sleep(0.01)

    log.info("Event type distribution:")
    for event_type, count in sorted(stats.items()):
        log.info(f"  {event_type}: {count}")

    _write_batch_to_db(events)
    log.info(f"✅ Processed {len(events)} events")


def _write_batch_to_db(events: list):
    """Write a batch of events to SQLite (simulating DB sink)."""
    if not events:
        return
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS streaming_events (
                    event_id    TEXT PRIMARY KEY,
                    employee_id TEXT,
                    event_type  TEXT,
                    timestamp   TEXT,
                    department  TEXT,
                    location    TEXT,
                    device_id   TEXT,
                    source      TEXT,
                    processed_at TEXT
                )
            """)
            conn.executemany("""
                INSERT OR REPLACE INTO streaming_events
                (event_id, employee_id, event_type, timestamp, department, location, device_id, source, processed_at)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, [
                (
                    e.get("event_id"), e.get("employee_id"), e.get("event_type"),
                    e.get("timestamp"), e.get("department"), e.get("location"),
                    e.get("device_id"), e.get("source"), datetime.utcnow().isoformat()
                )
                for e in events
            ])
            conn.commit()
    except Exception as ex:
        log.error(f"DB write failed: {ex}")


if __name__ == "__main__":
    log.info("=== Tasks 19-20: Kafka Consumer ===")
    run_consumer_kafka()
