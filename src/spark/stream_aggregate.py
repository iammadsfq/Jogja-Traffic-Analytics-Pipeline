import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum as spark_sum, to_timestamp, window
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

    spark = (
        SparkSession.builder.appName("jtap-stream-aggregation")
        .config("spark.sql.shuffle.partitions", "4")
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
            col("direction"),
        )
        .agg(spark_sum("confidence").alias("confidence_sum"))
    )

    query = (
        agg.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
