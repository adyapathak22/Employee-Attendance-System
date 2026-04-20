"""
src/streaming/streaming_sim.py
TASK 18 — Streaming Concepts: Simulate real-time data processing
"""
import time
import random
import json
import os
import threading
import queue
import logging
from datetime import datetime
from collections import defaultdict, deque

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)


class StreamingSimulator:
    """
    Simulates a real-time attendance event stream with:
    - Bounded in-memory queue (simulating Kafka topic)
    - Multiple producer threads
    - Consumer with windowed aggregation
    - Late data handling
    """

    def __init__(self, window_seconds: int = 10):
        self.event_queue = queue.Queue(maxsize=1000)
        self.window_seconds = window_seconds
        self.stats = defaultdict(int)
        self.window_buffer = deque()
        self.running = False

    def _generate_event(self) -> dict:
        depts = ["Engineering", "HR", "Finance", "Sales"]
        events = ["CHECK_IN", "CHECK_OUT", "LATE_ARRIVAL"]
        return {
            "event_id": f"EVT{random.randint(10000, 99999)}",
            "employee_id": f"EMP{random.randint(1, 200):04d}",
            "event_type": random.choices(events, weights=[0.45, 0.45, 0.1])[0],
            "timestamp": datetime.utcnow().isoformat(),
            "department": random.choice(depts),
            "processing_time": datetime.utcnow().timestamp(),
        }

    def producer(self, name: str, rate_per_sec: float = 2.0, num_events: int = 30):
        """Produces events into the queue."""
        for i in range(num_events):
            if not self.running:
                break
            event = self._generate_event()
            # Simulate occasional late events
            if random.random() < 0.05:
                event["timestamp"] = datetime(2024, 1, 1).isoformat()  # very old event
            try:
                self.event_queue.put(event, timeout=1)
                self.stats["produced"] += 1
            except queue.Full:
                self.stats["dropped"] += 1
            time.sleep(1.0 / rate_per_sec)
        log.info(f"  Producer [{name}] done.")

    def consumer(self, duration_sec: int = 15):
        """Consumes events, applies windowed aggregation."""
        window_counts = defaultdict(lambda: defaultdict(int))
        start = time.time()

        while time.time() - start < duration_sec or not self.event_queue.empty():
            try:
                event = self.event_queue.get(timeout=0.5)
                self.stats["consumed"] += 1

                # Windowed aggregation (tumbling window)
                window_key = int(event["processing_time"] // self.window_seconds)
                dept = event["department"]
                etype = event["event_type"]
                window_counts[window_key][f"{dept}:{etype}"] += 1

                # Late data detection
                event_ts = datetime.fromisoformat(event["timestamp"]).timestamp()
                lag = event["processing_time"] - event_ts
                if lag > 60:
                    self.stats["late_events"] += 1

                self.event_queue.task_done()
            except queue.Empty:
                continue

        # Print window results
        log.info("\nWindow Aggregation Results:")
        for wk, counts in sorted(window_counts.items()):
            window_start = datetime.fromtimestamp(wk * self.window_seconds).strftime("%H:%M:%S")
            log.info(f"  Window {window_start}:")
            for key, count in sorted(counts.items()):
                log.info(f"    {key}: {count} events")

    def run(self, num_producers: int = 2, duration_sec: int = 12):
        """Run the full streaming simulation."""
        log.info(f"\n=== Streaming Simulation ===")
        log.info(f"  Window: {self.window_seconds}s | Producers: {num_producers} | Duration: {duration_sec}s")
        self.running = True

        # Start producers in threads
        producers = []
        for i in range(num_producers):
            t = threading.Thread(
                target=self.producer,
                args=(f"P{i+1}", 3.0, 40),
                daemon=True,
            )
            t.start()
            producers.append(t)

        # Run consumer in main thread
        self.consumer(duration_sec)

        self.running = False
        for t in producers:
            t.join(timeout=2)

        log.info(f"\nStats: {dict(self.stats)}")
        log.info(f"  Queue size remaining: {self.event_queue.qsize()}")


def explain_streaming_concepts():
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║  STREAMING CONCEPTS — Attendance Real-time Processing       ║
    ╠══════════════════════════════════════════════════════════════╣
    ║                                                              ║
    ║  Batch vs Stream Processing:                                 ║
    ║  Batch: Process yesterday's attendance at 2 AM (high        ║
    ║         latency, high throughput)                            ║
    ║  Stream: Alert manager when employee is 1hr late (low       ║
    ║          latency, event-by-event)                            ║
    ║                                                              ║
    ║  Window Types:                                               ║
    ║  Tumbling: Non-overlapping 5-min windows                     ║
    ║    [09:00-09:05] [09:05-09:10] [09:10-09:15]                ║
    ║  Sliding:  Overlapping — every 1 min, 5-min window          ║
    ║    [09:00-09:05] [09:01-09:06] [09:02-09:07]                ║
    ║  Session:  Inactivity gap (e.g., 30min no events = new)     ║
    ║                                                              ║
    ║  Late Data:                                                  ║
    ║  - Events may arrive out-of-order due to network delays      ║
    ║  - Watermark: allow up to 10min late data before closing     ║
    ║  - Trade-off: completeness vs latency                        ║
    ║                                                              ║
    ║  Event Time vs Processing Time:                              ║
    ║  Event time:      when check-in actually happened            ║
    ║  Processing time: when Kafka/Spark received the event        ║
    ╚══════════════════════════════════════════════════════════════╝
    """)


def main():
    log.info("=== Task 18: Streaming Concepts ===\n")
    explain_streaming_concepts()
    sim = StreamingSimulator(window_seconds=5)
    sim.run(num_producers=2, duration_sec=10)
    log.info("✅ Task 18 Complete!")


if __name__ == "__main__":
    main()
