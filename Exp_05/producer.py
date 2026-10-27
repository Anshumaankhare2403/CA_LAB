"""
Experiment 05 — Event Hub Producer Script
==========================================
Sends simulated real-time student activity events to Azure Event Hub.

Usage:
    pip install azure-eventhub
    python producer.py

Replace <EVENT_HUB_CONNECTION_STRING> with your DatabricksPolicy connection string.
"""

import json
import time
import random
from datetime import datetime, timezone

from azure.eventhub import EventHubProducerClient, EventData

# ─── Configuration ──────────────────────────────────────────────────────────
CONNECTION_STRING = (
    "Endpoint=sb://calab-eventhub-ns-141.servicebus.windows.net/;"
    "SharedAccessKeyName=DatabricksPolicy;"
    "SharedAccessKey=<YOUR_SHARED_ACCESS_KEY>;"
    "EntityPath=calab-eventhub-141"
)
EVENT_HUB_NAME = "calab-eventhub-141"

# Student names and event types
STUDENTS = ["Abhi", "Kshitiz", "Aakriti", "Rohit", "Sandhya", "Riya"]
EVENT_TYPES = [
    "login", "logout", "quiz_submit",
    "assignment_upload", "attendance_mark", "lecture_view"
]
COURSES = ["Cloud Analytics", "Machine Learning", "Data Engineering"]

# ─── Event Generator ────────────────────────────────────────────────────────

def generate_event():
    """Generate a single simulated student activity event."""
    student = random.choice(STUDENTS)
    event_type = random.choice(EVENT_TYPES)

    event = {
        "student_name": student,
        "event_type": event_type,
        "course": random.choice(COURSES),
        "score": random.randint(40, 100) if event_type == "quiz_submit" else None,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "session_id": f"sess-{random.randint(1000, 9999)}",
        "device": random.choice(["laptop", "mobile", "tablet"]),
    }
    return event


def send_events(num_events=50, delay_seconds=1.0):
    """
    Send a stream of events to Azure Event Hub.

    Args:
        num_events: Total number of events to send (0 = infinite).
        delay_seconds: Delay between events in seconds.
    """
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        eventhub_name=EVENT_HUB_NAME
    )

    print(f"[Producer] Connected to Event Hub: {EVENT_HUB_NAME}")
    print(f"[Producer] Sending {num_events} events (delay={delay_seconds}s each)...")
    print("-" * 60)

    sent_count = 0
    try:
        while num_events == 0 or sent_count < num_events:
            # Create a batch of events
            event_data_batch = producer.create_batch()

            # Generate and add event to batch
            event = generate_event()
            event_json = json.dumps(event)
            event_data_batch.add(EventData(event_json))

            # Send the batch
            producer.send_batch(event_data_batch)
            sent_count += 1

            print(
                f"  [{sent_count:>4}] {event['student_name']:<10} | "
                f"{event['event_type']:<20} | "
                f"Score: {event['score'] or 'N/A':<5} | "
                f"{event['timestamp']}"
            )

            time.sleep(delay_seconds)

    except KeyboardInterrupt:
        print(f"\n[Producer] Interrupted — sent {sent_count} events total.")
    except Exception as e:
        print(f"\n[Producer] Error: {e}")
    finally:
        producer.close()
        print(f"[Producer] Connection closed. Total events sent: {sent_count}")


# ─── Main ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  Azure Event Hub — Student Activity Event Producer")
    print(f"  Namespace:  calab-eventhub-ns-141")
    print(f"  Event Hub:  {EVENT_HUB_NAME}")
    print(f"  Students:   {', '.join(STUDENTS)}")
    print("=" * 60)

    send_events(num_events=50, delay_seconds=1.0)
