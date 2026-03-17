import os

import psycopg
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    lit,
    lower,
    to_timestamp,
    window,
)
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("camera_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("object_type", StringType(), True),
        StructField("direction", StringType(), True),
        StructField("confidence", DoubleType(), True),
        StructField(
            "metadata",
            StructType(
                [
                    StructField("location", StringType(), True),
                    StructField("lane_id", IntegerType(), True),
                ]
            ),
            True,
        ),
    ]
)


def main() -> None:
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC_RAW", "traffic.raw")
    pg_host = os.getenv("POSTGRES_HOST", "localhost")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    pg_db = os.getenv("POSTGRES_DB", "traffic_analytics")
    pg_user = os.getenv("POSTGRES_USER", "traffic_user")
    pg_password = os.getenv("POSTGRES_PASSWORD", "traffic_password")
    checkpoint_dir = os.getenv("SPARK_CHECKPOINT_DIR", "./.spark-checkpoints/traffic_agg_1m")

    pg_dsn = (
        f"host={pg_host} port={pg_port} dbname={pg_db} user={pg_user} "
        f"password={pg_password}"
    )

    spark = (
        SparkSession.builder.appName("jtap-stream-aggregation")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("timestamp")))
    )

    agg = (
        parsed.withWatermark("event_time", "2 minutes")
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("camera_id"),
            col("object_type"),
            lower(col("direction")).alias("direction"),
            col("metadata.location").alias("location"),
            col("metadata.lane_id").alias("lane_id"),
        )
        .agg(
            count(lit(1)).alias("total_events"),
            avg("confidence").alias("avg_confidence"),
        )
        .select(
            col("window.start").alias("bucket"),
            col("camera_id"),
            col("object_type"),
            col("direction"),
            col("location"),
            col("lane_id"),
            col("total_events"),
            col("avg_confidence"),
        )
    )

    def write_batch(batch_df, batch_id: int) -> None:
        rows = [
            (
                row["bucket"],
                row["camera_id"],
                row["object_type"],
                row["direction"] or "unknown",
                row["location"] or "unknown",
                int(row["lane_id"] if row["lane_id"] is not None else -1),
                int(row["total_events"]),
                float(row["avg_confidence"]) if row["avg_confidence"] is not None else None,
            )
            for row in batch_df.collect()
        ]

        if not rows:
            print(f"batch={batch_id} no rows to write")
            return

        upsert_sql = """
            INSERT INTO fact_traffic_agg_1m (
                bucket,
                camera_id,
                object_type,
                direction,
                location,
                lane_id,
                total_events,
                avg_confidence
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (bucket, camera_id, object_type, direction, location, lane_id)
            DO UPDATE SET
                total_events = EXCLUDED.total_events,
                avg_confidence = EXCLUDED.avg_confidence,
                updated_at = NOW();
        """

        with psycopg.connect(pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.executemany(upsert_sql, rows)
            conn.commit()

        print(f"batch={batch_id} rows_upserted={len(rows)}")

    query = (
        agg.writeStream.outputMode("update")
        .option("checkpointLocation", checkpoint_dir)
        .foreachBatch(write_batch)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
