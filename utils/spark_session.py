"""utils/spark_session.py — Centralised SparkSession factory driven by platform.yaml."""
from __future__ import annotations
import yaml
from pathlib import Path
from pyspark.sql import SparkSession

def get_spark(config_path: str | Path | None = None) -> SparkSession:
    path = Path(config_path) if config_path else Path(__file__).parents[1] / "config" / "platform.yaml"
    with open(path) as f:
        cfg = yaml.safe_load(f)
    sc = cfg.get("spark", {})
    builder = SparkSession.builder.appName(sc.get("app_name", "astra")).master(sc.get("master", "local[*]"))
    for k, v in sc.get("configs", {}).items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(cfg.get("logging", {}).get("level", "WARN"))
    return spark
