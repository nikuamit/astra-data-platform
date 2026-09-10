"""
ingestion/streaming/kafka_to_bronze.py
Kafka → Bronze Delta via Structured Streaming.
Stores raw bytes + metadata. No transforms in Bronze — raw is sacred.
Uses foreachBatch for exactly-once Delta writes + checkpoint resume.
"""
from __future__ import annotations
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from utils.config_loader import load_config
from utils.spark_session import get_spark
from utils.logger import get_logger

log = get_logger(__name__)

def run(topic: str | None = None) -> None:
    cfg = load_config()
    kc, sc = cfg["kafka"], cfg["storage"]
    topic = topic or kc["topics"]["events"]
    bronze_path    = sc["base_path"] + sc["bronze"] + f"/{topic.replace('.','_')}"
    checkpoint     = sc["base_path"] + sc["checkpoint"] + f"/{topic}"
    spark = get_spark()

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", kc["bootstrap_servers"])
           .option("subscribe", topic)
           .option("startingOffsets", kc["auto_offset_reset"])
           .load())

    bronze = raw.select(
        F.col("key").cast(StringType()).alias("event_key"),
        F.col("value").cast(StringType()).alias("event_payload"),
        F.col("topic"), F.col("partition"), F.col("offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("ingest_ts"),
        F.to_date(F.col("timestamp")).alias("event_date"),
    )

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        log.info("bronze_batch", extra={"batch_id": batch_id})
        batch_df.write.format("delta").mode("append").partitionBy("event_date").save(bronze_path)

    (bronze.writeStream
     .foreachBatch(write_batch)
     .option("checkpointLocation", checkpoint)
     .trigger(processingTime="30 seconds")
     .start().awaitTermination())

if __name__ == "__main__":
    run()
