
import json
import time
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

TOPIC = "campus-events"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all"
)

# Event templates
events = [
    ("S101", "check-in", "Computer Science"),
    ("S204", "check-in", "Mathematics"),
    ("S101", "open-course", "Computer Science"),
    ("S305", "check-in", "Physics"),
    ("S411", "check-in", "Computer Science"),
    ("S204", "check-in", "Mathematics"),
    ("S512", "check-in", "Physics"),
    ("S512", "library-entry", "Computer Science")
]

TOTAL_EVENTS = 100

base = datetime.now(timezone.utc).replace(microsecond=0)

futures = []
start_time = time.perf_counter()

try:
    for i in range(TOTAL_EVENTS):
        student, event_type, department = events[i % len(events)]

        # Generate event timestamps across 100 seconds
        offset_ms = i * 1000

        # Event 7 is deliberately out of order
        if i == 6:
            offset_ms = 200

        event_time = base + timedelta(milliseconds=offset_ms)

        event = {
            "eventId": i + 1,
            "studentId": student,
            "eventType": event_type,
            "department": department,
            "eventTime": event_time.isoformat()
        }

        future = producer.send(TOPIC, value=event)
        futures.append(future)

        print("Sent:", event)

        time.sleep(0.05)

    # Wait for Kafka acknowledgments
    for future in futures:
        future.get(timeout=30)

    producer.flush()

    elapsed = time.perf_counter() - start_time
    ingestion_rate = TOTAL_EVENTS / elapsed

    print("\n========== Producer Summary ==========")
    print("Total events sent:", TOTAL_EVENTS)
    print("Total time:", round(elapsed, 2), "seconds")
    print("Ingestion rate:", round(ingestion_rate, 2), "events/second")

finally:
    producer.close()
