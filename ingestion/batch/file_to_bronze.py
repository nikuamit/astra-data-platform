"""
ingestion/batch/file_to_bronze.py
Batch file ingestion: CSV / Parquet / JSON / Avro → Bronze Delta.
Adds audit columns (_ingest_ts, _source_path, _batch_date).
Usage: python -m ingestion.batch.file_to_bronze --source s3a://raw/ --entity events --format parquet
"""
from __future__ import annotations
import argparse
from datetime import date
import pyspark.sql.functions as F
from utils.config_loader import load_config
from utils.spark_session import get_spark
from utils.logger import get_logger

log = get_logger(__name__)
SUPPORTED = {"parquet", "csv", "json", "avro", "orc"}

def ingest(source_path: str, entity: str, fmt: str = "parquet", options: dict | None = None) -> None:
    cfg = load_config()
    bronze_path = cfg["storage"]["base_path"] + cfg["storage"]["bronze"] + f"/{entity}"
    if fmt not in SUPPORTED:
        raise ValueError(f"Unsupported format '{fmt}'. Choose from {SUPPORTED}")
    spark = get_spark()
    reader = spark.read.format(fmt)
    for k, v in (options or {}).items():
        reader = reader.option(k, v)
    df = reader.load(source_path)
    enriched = df.select("*",
        F.current_timestamp().alias("_ingest_ts"),
        F.lit(source_path).alias("_source_path"),
        F.lit(str(date.today())).alias("_batch_date"),
    )
    log.info("ingest_start", extra={"entity": entity, "rows": enriched.count()})
    enriched.write.format("delta").mode("append").partitionBy("_batch_date").save(bronze_path)
    log.info("ingest_done", extra={"entity": entity, "path": bronze_path})

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True)
    p.add_argument("--entity", required=True)
    p.add_argument("--format", default="parquet", dest="fmt")
    p.add_argument("--option", action="append", default=[], metavar="KEY=VALUE")
    args = p.parse_args()
    ingest(args.source, args.entity, args.fmt, dict(o.split("=", 1) for o in args.option))
