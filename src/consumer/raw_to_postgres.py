import json
import os

from dotenv import load_dotenv
from kafka import KafkaConsumer
from psycopg import connect

INSERT_SQL = """
INSERT INTO fact_traffic_events (
    time,
    camera_id,
    object_type,
    direction,
    event_count,
    event_id,
    confidence,
    location,
    lane_id
)
VALUES (%s, %s, %s, %s, %s, %s::uuid, %s, %s, %s);
"""


def parse_event(event: dict) -> tuple:
    metadata = event.get("metadata", {})
    return (
        event.get("timestamp"),
        event.get("camera_id"),
        event.get("object_type"),
        event.get("direction"),
        1,
        event.get("event_id"),
        event.get("confidence"),
        metadata.get("location"),
        metadata.get("lane_id"),
    )


def main() -> None:
    load_dotenv()

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC_RAW", "traffic.raw")
    batch_size = int(os.getenv("DB_WRITE_BATCH_SIZE", "100"))

    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = int(os.getenv("POSTGRES_PORT", "5432"))
    db_name = os.getenv("POSTGRES_DB", "traffic_analytics")
    db_user = os.getenv("POSTGRES_USER", "traffic_user")
    db_password = os.getenv("POSTGRES_PASSWORD", "traffic_password")

    conn = connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password,
    )
    conn.autocommit = False

    # At-least-once delivery: commit Kafka offsets only after the DB write is
    # durably committed (see batch flush below). auto_offset_reset=earliest so a
    # fresh consumer group replays the backlog instead of skipping it.
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="jtap-raw-to-postgres",
    )

    print(f"Consuming topic={topic} and writing to PostgreSQL in batches of {batch_size}")

    buffer = []
    try:
        with conn.cursor() as cur:
            for msg in consumer:
                buffer.append(parse_event(msg.value))
                if len(buffer) >= batch_size:
                    cur.executemany(INSERT_SQL, buffer)
                    conn.commit()
                    # Offsets advance only after the rows are durably committed.
                    consumer.commit()
                    buffer.clear()
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        if buffer:
            with conn.cursor() as cur:
                cur.executemany(INSERT_SQL, buffer)
                conn.commit()
            consumer.commit()
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
