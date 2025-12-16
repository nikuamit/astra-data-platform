import yaml
from pyspark.sql import functions as F
from utils.spark_session import get_spark_session
from utils.logger import get_logger

logger = get_logger("events_stream")

def load_config():
    with open("config/dev.yaml", "r") as f:
        return yaml.safe_load(f)

def main():
    config = load_config()

    spark = get_spark_session(
        app_name=config["spark"]["app_name"],
        master=config["spark"]["master"]
    )

    logger.info("Spark session started")

    # Simulated streaming source
    events_df = (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", 5)
        .load()
        .withColumn("event_type", F.lit("page_view"))
        .withColumn("user_id", (F.rand() * 1000).cast("int"))
        .withColumn("event_ts", F.current_timestamp())
    )

    logger.info("Streaming source initialized")

    query = (
        events_df.writeStream
        .format("parquet")
        .option("checkpointLocation", "data/bronze/_checkpoints/events")
        .option("path", config["storage"]["bronze"] + "/events")
        .outputMode("append")
        .start()
    )

    logger.info("Streaming write started to Bronze layer")
    query.awaitTermination()

if __name__ == "__main__":
    main()
