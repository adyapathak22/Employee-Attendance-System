"""
kafka/producer.py
TASKS 18, 19, 20 — Streaming Concepts, Kafka Basics, Kafka Advanced
Real-time attendance check-in event stream producer
"""
import json
import time
import random
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

TOPIC = "attendance-events"
EMPLOYEES = [f"EMP{i:04d}" for i in range(1, 201)]
DEPARTMENTS = ["Engineering", "HR", "Finance", "Sales", "Marketing", "IT Support"]
EVENTS = ["CHECK_IN", "CHECK_OUT", "BREAK_START", "BREAK_END", "OVERTIME_START"]
EVENT_WEIGHTS = [0.4, 0.4, 0.08, 0.08, 0.04]


def generate_attendance_event() -> dict:
    """Generate a realistic attendance check-in/out event."""
    emp = random.choice(EMPLOYEES)
    event_type = random.choices(EVENTS, weights=EVENT_WEIGHTS)[0]
    return {
        "event_id": f"EVT{random.randint(100000, 999999)}",
        "employee_id": emp,
        "event_type": event_type,
        "timestamp": datetime.utcnow().isoformat(),
        "department": random.choice(DEPARTMENTS),
        "location": random.choice(["Floor-1", "Floor-2", "Remote", "Floor-3"]),
        "device_id": f"DEVICE{random.randint(1, 20):03d}",
        "latitude": round(26.8467 + random.gauss(0, 0.01), 6),
        "longitude": round(80.9462 + random.gauss(0, 0.01), 6),
        "source": random.choice(["biometric", "mobile_app", "web_portal"]),
    }


def run_producer_kafka(num_events: int = 100, delay: float = 0.1):
    """Kafka producer — sends attendance events to topic."""
    try:
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            # Task 20: Partitioning config
            acks="all",              # Wait for all replicas
            retries=3,
            batch_size=16384,        # 16KB batches
            linger_ms=10,            # Wait 10ms to batch messages
            compression_type="gzip",
        )
        log.info(f"Kafka producer connected. Sending {num_events} events to '{TOPIC}'...")

        for i in range(num_events):
            event = generate_attendance_event()
            # Use employee_id as partition key (same employee → same partition)
            producer.send(TOPIC, key=event["employee_id"], value=event)

            if (i + 1) % 10 == 0:
                log.info(f"  Sent {i+1}/{num_events} events")
            time.sleep(delay)

        producer.flush()
        log.info(f"✅ Sent {num_events} events to Kafka topic '{TOPIC}'")
        producer.close()

    except ImportError:
        log.warning("kafka-python not installed. Running simulation mode...")
        run_producer_simulated(num_events, delay)
    except Exception as e:
        log.warning(f"Kafka unavailable ({e}). Running simulation mode...")
        run_producer_simulated(num_events, delay)


def run_producer_simulated(num_events: int = 20, delay: float = 0.2):
    """Simulated producer — writes to local JSON file (no Kafka needed)."""
    import os
    os.makedirs("data/streaming", exist_ok=True)

    log.info(f"SIMULATION MODE: Generating {num_events} events...")
    events = []
    for i in range(num_events):
        event = generate_attendance_event()
        events.append(event)
        log.info(f"  [{i+1:03d}] {event['event_type']} | {event['employee_id']} | {event['timestamp']}")
        time.sleep(delay)

    with open("data/streaming/simulated_events.json", "w") as f:
        json.dump(events, f, indent=2)
    log.info(f"  Saved {len(events)} events → data/streaming/simulated_events.json")


def explain_kafka_concepts():
    print("""
    ╔══════════════════════════════════════════════════════════════════╗
    ║  Kafka Architecture for Attendance System                       ║
    ╠══════════════════════════════════════════════════════════════════╣
    ║                                                                  ║
    ║  Topic: attendance-events (3 partitions, replication=2)         ║
    ║                                                                  ║
    ║  Producers:                                                      ║
    ║  - Biometric scanner → CHECK_IN/CHECK_OUT events                 ║
    ║  - Mobile app → remote attendance events                         ║
    ║  - Web portal → manual attendance corrections                    ║
    ║                                                                  ║
    ║  Partitioning strategy: employee_id % num_partitions             ║
    ║  → Guarantees ordered events per employee                        ║
    ║                                                                  ║
    ║  Consumers:                                                      ║
    ║  - attendance-db-writer (group: db-writers)                      ║
    ║  - attendance-alert-service (group: alerting)                    ║
    ║  - spark-streaming-job (group: analytics)                        ║
    ║                                                                  ║
    ║  Offset Management:                                              ║
    ║  - Committed offsets stored in __consumer_offsets                ║
    ║  - enable.auto.commit=false for exactly-once semantics          ║
    ║  - Manual commit after DB write succeeds                         ║
    ║                                                                  ║
    ║  Retention: 7 days (604800000 ms)                                ║
    ╚══════════════════════════════════════════════════════════════════╝
    """)


if __name__ == "__main__":
    log.info("=== Tasks 18-20: Kafka Producer ===")
    explain_kafka_concepts()
    run_producer_kafka(num_events=50, delay=0.05)
