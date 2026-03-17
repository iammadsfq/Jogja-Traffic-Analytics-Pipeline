import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer


def duplicate_event(event: dict, camera_ids: list[str]) -> dict:
    out = dict(event)
    out["event_id"] = str(uuid.uuid4())
    out["camera_id"] = random.choice(camera_ids)
    out["timestamp"] = datetime.now(timezone.utc).isoformat()
    return out


def main() -> None:
    load_dotenv()

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    source_topic = os.getenv("KAFKA_TOPIC_SOURCE", "traffic.seed")
    target_topic = os.getenv("KAFKA_TOPIC_TARGET", os.getenv("KAFKA_TOPIC_RAW", "traffic.raw"))

    camera_count = int(os.getenv("MOCK_CAMERA_COUNT", "100"))
    duplicate_factor = int(os.getenv("DUPLICATE_FACTOR", "5"))
    max_out_eps = float(os.getenv("MAX_OUT_EPS", "1000"))
    camera_prefix = os.getenv("MOCK_CAMERA_PREFIX", "cam")

    camera_ids = [f"{camera_prefix}_{i:03d}" for i in range(1, camera_count + 1)]

    consumer = KafkaConsumer(
        source_topic,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="jtap-mock-duplicator",
    )

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=20,
        acks="all",
    )

    sleep_interval = 1.0 / max(max_out_eps, 1.0)
    print(
        "Mock duplicator running: "
        f"source={source_topic}, target={target_topic}, "
        f"camera_count={camera_count}, duplicate_factor={duplicate_factor}, "
        f"max_out_eps={max_out_eps}"
    )

    try:
        for msg in consumer:
            base_event = msg.value
            for _ in range(duplicate_factor):
                dup = duplicate_event(base_event, camera_ids)
                producer.send(target_topic, value=dup)
                time.sleep(sleep_interval)
    except KeyboardInterrupt:
        print("Stopping mock duplicator...")
    finally:
        producer.flush()
        producer.close()
        consumer.close()


if __name__ == "__main__":
    main()
