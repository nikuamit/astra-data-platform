"""
ingestion/streaming/kafka_to_bronze.py
Reads raw events from Kafka and lands them in the Bronze Delta layer.

Design decisions:
- Stores raw bytes + Kafka metadata — no transforms in Bronze.
- Uses foreachBatch for exactly-once Delta writes.
- Checkpoint path ensures resume on restart.
"""

from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

from utils.config_loader import load_config
from utils.delta_writer import write_delta
from utils.logger import get_logger
from utils.spark_session import get_spark

log = get_logger(__name__)


def run(topic: str | None = None) -> None:
    cfg = load_config()
    kafka_cfg = cfg["kafka"]
    storage_cfg = cfg["storage"]

    topic = topic or kafka_cfg["topics"]["events"]
    bronze_path = storage_cfg["base_path"] + storage_cfg["bronze"] + f"/{topic.replace('.', '_')}"
    checkpoint_path = storage_cfg["base_path"] + storage_cfg["checkpoint"] + f"/{topic}"

    spark = get_spark()

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_cfg["bootstrap_servers"])
        .option("subscribe", topic)
        .option("startingOffsets", kafka_cfg["auto_offset_reset"])
        .option("kafka.group.id", kafka_cfg["group_id"])
        .load()
    )

    bronze_stream = raw_stream.select(
        F.col("key").cast(StringType()).alias("event_key"),
        F.col("value").cast(StringType()).alias("event_payload"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("ingest_ts"),
        F.to_date(F.col("timestamp")).alias("event_date"),   # partition column
    )

    def write_bronze_batch(batch_df: DataFrame, batch_id: int) -> None:
        log.info("writing bronze batch", extra={"batch_id": batch_id, "topic": topic})
        write_delta(batch_df, bronze_path, mode="append", partition_by=["event_date"])

    query = (
        bronze_stream.writeStream
        .foreachBatch(write_bronze_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="30 seconds")
        .start()
    )

    log.info("streaming query started", extra={"topic": topic, "bronze_path": bronze_path})
    query.awaitTermination()


if __name__ == "__main__":
    run()
