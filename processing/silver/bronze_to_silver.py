"""
processing/silver/bronze_to_silver.py
Bronze → Silver: parse JSON, enforce schema, deduplicate, run DQ, quarantine bad rows.
Entity config (schema + DQ rules) loaded from config/entities/<entity>.yaml.
"""
from __future__ import annotations
import argparse
from pathlib import Path
import yaml
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from dq.models import DQRule, RuleType
from dq.quality_engine import QualityEngine
from utils.config_loader import load_config
from utils.spark_session import get_spark
from utils.logger import get_logger

log = get_logger(__name__)

def load_entity_config(entity: str) -> dict:
    p = Path(__file__).parents[2] / "config" / "entities" / f"{entity}.yaml"
    return yaml.safe_load(p.read_text()) if p.exists() else {}

def process(entity: str, batch_date: str | None = None) -> None:
    cfg = load_config()
    sc, dq_cfg = cfg["storage"], cfg["data_quality"]
    bronze_path    = sc["base_path"] + sc["bronze"]    + f"/{entity}"
    silver_path    = sc["base_path"] + sc["silver"]    + f"/{entity}"
    quarantine     = sc["base_path"] + sc["quarantine"] + f"/{entity}"

    entity_cfg = load_entity_config(entity)
    spark = get_spark()
    df: DataFrame = spark.read.format("delta").load(bronze_path)
    if batch_date:
        df = df.filter(F.col("_batch_date") == batch_date)

    # Deduplicate: keep latest per natural key
    dedup_keys = entity_cfg.get("dedup_keys", [])
    if dedup_keys:
        w = Window.partitionBy(*dedup_keys).orderBy(F.col("_ingest_ts").desc())
        df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    # DQ
    rules = [DQRule(name=r["name"], rule_type=RuleType(r["type"]),
                    column=r.get("column"), params=r.get("params", {}))
             for r in entity_cfg.get("dq_rules", [])]
    engine = QualityEngine(rules=rules,
                           fail_on_error=dq_cfg.get("fail_on_error", False),
                           alert_threshold=dq_cfg.get("alert_threshold", 0.05))
    clean_df, results = engine.run(df, quarantine_path=quarantine)
    log.info("dq_summary", extra={"entity": entity, "results": [r.__dict__ for r in results]})

    (clean_df.withColumn("_silver_ts", F.current_timestamp())
     .write.format("delta").mode("append").partitionBy("_batch_date").save(silver_path))
    log.info("silver_done", extra={"entity": entity})

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--entity", required=True)
    p.add_argument("--batch-date", default=None)
    args = p.parse_args()
    process(args.entity, args.batch_date)
