"""
ingestion/batch/file_to_bronze.py
Batch ingestion: read CSV / Parquet / JSON from a source path and
write to Bronze Delta with audit columns.

Usage:
    python -m ingestion.batch.file_to_bronze \
        --source s3a://raw/events/2024-01-01/ \
        --format parquet \
        --entity events
"""

from __future__ import annotations

import argparse
from datetime import date

import pyspark.sql.functions as F

from utils.config_loader import load_config
from utils.delta_writer import write_delta
from utils.logger import get_logger
from utils.spark_session import get_spark

log = get_logger(__name__)

SUPPORTED_FORMATS = {"parquet", "csv", "json", "avro", "orc"}


def ingest(
    source_path: str,
    entity: str,
    fmt: str = "parquet",
    options: dict | None = None,
) -> None:
    cfg = load_config()
    storage = cfg["storage"]
    bronze_path = storage["base_path"] + storage["bronze"] + f"/{entity}"

    if fmt not in SUPPORTED_FORMATS:
        raise ValueError(f"Unsupported format '{fmt}'. Choose from {SUPPORTED_FORMATS}")

    spark = get_spark()
    reader = spark.read.format(fmt)
    if options:
        for k, v in options.items():
            reader = reader.option(k, v)

    df = reader.load(source_path)

    enriched = df.select(
        "*",
        F.current_timestamp().alias("_ingest_ts"),
        F.lit(source_path).alias("_source_path"),
        F.lit(str(date.today())).alias("_batch_date"),
    )

    log.info("starting batch ingest", extra={"entity": entity, "source": source_path, "rows": enriched.count()})
    write_delta(enriched, bronze_path, mode="append", partition_by=["_batch_date"])
    log.info("batch ingest complete", extra={"entity": entity, "bronze_path": bronze_path})


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Batch file → Bronze ingestion")
    p.add_argument("--source", required=True)
    p.add_argument("--entity", required=True)
    p.add_argument("--format", default="parquet", dest="fmt")
    p.add_argument("--option", action="append", default=[], metavar="KEY=VALUE")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    opts = dict(o.split("=", 1) for o in args.option)
    ingest(args.source, args.entity, args.fmt, opts)
