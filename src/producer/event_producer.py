import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer

OBJECT_TYPES = ["car", "motorcycle", "person", "bus", "truck"]
DIRECTIONS = ["in", "out"]


def build_event(camera_id: str, location: str, lane_id: int) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "camera_id": camera_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "object_type": random.choice(OBJECT_TYPES),
        "direction": random.choice(DIRECTIONS),
        "confidence": round(random.uniform(0.65, 0.99), 4),
        "metadata": {
            "location": location,
            "lane_id": lane_id,
        },
    }


def main() -> None:
    load_dotenv()

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC_RAW", "traffic.raw")
    camera_id = os.getenv("CAMERA_ID", "cam_001")
    location = os.getenv("CAMERA_LOCATION", "jogja-default")
    lane_id = int(os.getenv("CAMERA_LANE_ID", "1"))
    events_per_second = float(os.getenv("EVENTS_PER_SECOND", "10"))

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=20,
        acks="all",
    )

    interval = 1.0 / max(events_per_second, 0.1)
    print(f"Producing to topic={topic} at ~{events_per_second} eps")

    try:
        while True:
            event = build_event(camera_id=camera_id, location=location, lane_id=lane_id)
            producer.send(topic, value=event)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
