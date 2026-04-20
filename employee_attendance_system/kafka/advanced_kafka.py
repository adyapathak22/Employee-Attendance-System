"""
kafka/advanced_kafka.py
TASK 20 — Kafka Advanced: Partitioning, offset management, consumer groups
"""
import json
import logging
import time
import random
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)


def explain_partitioning():
    """Demonstrate Kafka partitioning strategy for attendance events."""
    print("""
    ╔══════════════════════════════════════════════════════════════════╗
    ║  KAFKA PARTITIONING STRATEGY — attendance-events topic          ║
    ╠══════════════════════════════════════════════════════════════════╣
    ║                                                                  ║
    ║  Topic: attendance-events (3 partitions)                        ║
    ║                                                                  ║
    ║  Partition Assignment (key = employee_id):                      ║
    ║  hash("EMP0001") % 3 = 1 → Partition 1                         ║
    ║  hash("EMP0002") % 3 = 2 → Partition 2                         ║
    ║  hash("EMP0003") % 3 = 0 → Partition 0                         ║
    ║                                                                  ║
    ║  Guarantees: all events for EMP0001 arrive in order             ║
    ║  within Partition 1                                              ║
    ║                                                                  ║
    ║  Consumer Groups:                                                ║
    ║  Group "db-writers"   → 3 consumers (1 per partition)           ║
    ║  Group "alerting"     → 1 consumer (reads all partitions)       ║
    ║  Group "spark-stream" → 2 consumers (partitions split 2+1)      ║
    ║                                                                  ║
    ║  Offset Management:                                              ║
    ║  - __consumer_offsets internal topic tracks committed positions  ║
    ║  - enable.auto.commit=false → manual commit after processing     ║
    ║  - Exactly-once: idempotent producer + transactional API        ║
    ╚══════════════════════════════════════════════════════════════════╝
    """)


def simulate_consumer_group():
    """Simulate multiple consumers in a group processing partitions."""
    import os, json
    events_file = "data/streaming/simulated_events.json"
    if not os.path.exists(events_file):
        log.warning("Run kafka/producer.py first to generate events.")
        return

    with open(events_file) as f:
        events = json.load(f)

    # Assign events to 3 partitions based on employee_id hash
    partitions = {0: [], 1: [], 2: []}
    for event in events:
        p = hash(event["employee_id"]) % 3
        partitions[p].append(event)

    log.info(f"Partition distribution:")
    for p, evts in partitions.items():
        log.info(f"  Partition {p}: {len(evts)} events")

    # Simulate 3 consumers in a group
    log.info("\nSimulating consumer group processing...")
    offsets = {0: 0, 1: 0, 2: 0}
    for p, evts in partitions.items():
        log.info(f"\nConsumer-{p} processing Partition {p}:")
        for i, event in enumerate(evts[:5]):  # Show first 5 per partition
            log.info(f"  offset={i} | {event['event_type']} | {event['employee_id']}")
            offsets[p] = i + 1
        log.info(f"  Committed offset: {offsets[p]}")

    log.info(f"\nFinal committed offsets: {offsets}")


def demonstrate_exactly_once():
    """Explain and demonstrate exactly-once semantics."""
    log.info("\nExactly-Once Delivery Semantics:")
    log.info("""
  Three delivery guarantees in Kafka:
  ┌──────────────┬──────────────────────────────────────────────┐
  │ At-most-once  │ fire-and-forget, may lose messages           │
  │ At-least-once │ retry on failure, may produce duplicates     │
  │ Exactly-once  │ no loss, no duplicates (idempotent + txn)    │
  └──────────────┴──────────────────────────────────────────────┘

  Config for exactly-once producer:
    enable.idempotence=true
    acks=all
    max.in.flight.requests.per.connection=5
    transactional.id=attendance-producer-1

  Config for exactly-once consumer:
    isolation.level=read_committed
    enable.auto.commit=false
    (commit offset only after successful DB write)
    """)


if __name__ == "__main__":
    log.info("=== Task 20: Kafka Advanced ===\n")
    explain_partitioning()
    simulate_consumer_group()
    demonstrate_exactly_once()
    log.info("✅ Task 20 Complete!")
