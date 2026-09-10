"""
processing/silver/bronze_to_silver.py
Bronze → Silver: parse JSON payload, apply schema, run DQ, deduplicate.

Entity config (schema + rules) is loaded from config/entities/<entity>.yaml.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import yaml
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from dq.quality_engine import DQRule, QualityEngine, RuleType
from utils.config_loader import load_config
from utils.delta_writer import write_delta
from utils.logger import get_logger
from utils.spark_session import get_spark

log = get_logger(__name__)


def load_entity_config(entity: str) -> dict:
    path = Path(__file__).parents[2] / "config" / "entities" / f"{entity}.yaml"
    if not path.exists():
        return {}
    with open(path) as f:
        return yaml.safe_load(f)


def build_dq_rules(entity_cfg: dict) -> list[DQRule]:
    rules = []
    for r in entity_cfg.get("dq_rules", []):
        rules.append(
            DQRule(
                name=r["name"],
                rule_type=RuleType(r["type"]),
                column=r.get("column"),
                params=r.get("params", {}),
            )
        )
    return rules


def process(entity: str, batch_date: str | None = None) -> None:
    cfg = load_config()
    storage = cfg["storage"]
    dq_cfg = cfg["data_quality"]

    bronze_path = storage["base_path"] + storage["bronze"] + f"/{entity}"
    silver_path = storage["base_path"] + storage["silver"] + f"/{entity}"
    quarantine_path = storage["base_path"] + storage["quarantine"] + f"/{entity}"

    entity_cfg = load_entity_config(entity)
    spark = get_spark()

    reader = spark.read.format("delta")
    if batch_date:
        bronze_path_filtered = f"{bronze_path}/_batch_date={batch_date}"
        df: DataFrame = reader.load(bronze_path_filtered)
    else:
        df = reader.load(bronze_path)

    # Parse JSON payload if Bronze stores raw strings
    if "event_payload" in df.columns:
        schema_str = entity_cfg.get("json_schema")
        if schema_str:
            json_schema = StructType.fromJson(yaml.safe_load(schema_str))
            df = df.withColumn("_parsed", F.from_json(F.col("event_payload"), json_schema))
            df = df.select("*", F.col("_parsed.*")).drop("_parsed", "event_payload")

    # Deduplication
    dedup_keys = entity_cfg.get("dedup_keys", [])
    order_col = entity_cfg.get("dedup_order_col", "_ingest_ts")
    if dedup_keys and order_col in df.columns:
        from pyspark.sql.window import Window
        w = Window.partitionBy(*dedup_keys).orderBy(F.col(order_col).desc())
        df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    # DQ
    rules = build_dq_rules(entity_cfg)
    engine = QualityEngine(
        rules=rules,
        fail_on_error=dq_cfg.get("fail_on_error", False),
        alert_threshold=dq_cfg.get("alert_threshold", 0.05),
    )
    clean_df, dq_results = engine.run(df, quarantine_path=quarantine_path)

    log.info("dq summary", extra={"entity": entity, "results": [r.__dict__ for r in dq_results]})

    # Add silver audit cols
    silver_df = clean_df.withColumn("_silver_ts", F.current_timestamp())

    write_delta(silver_df, silver_path, mode="append", partition_by=["_batch_date"])
    log.info("silver write complete", extra={"entity": entity, "path": silver_path})


def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--entity", required=True)
    p.add_argument("--batch-date", default=None)
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    process(args.entity, args.batch_date)
