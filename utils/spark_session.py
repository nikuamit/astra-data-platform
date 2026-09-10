"""
utils/spark_session.py
Centralised SparkSession factory driven by config/platform.yaml.
"""

from __future__ import annotations

import yaml
from pathlib import Path
from pyspark.sql import SparkSession


def _load_config(path: str | Path | None = None) -> dict:
    config_path = Path(path) if path else Path(__file__).parents[1] / "config" / "platform.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


def get_spark(config_path: str | Path | None = None) -> SparkSession:
    """Return (or reuse) a SparkSession configured from platform.yaml."""
    cfg = _load_config(config_path)
    spark_cfg = cfg.get("spark", {})

    builder = (
        SparkSession.builder
        .appName(spark_cfg.get("app_name", "astra"))
        .master(spark_cfg.get("master", "local[*]"))
    )

    for key, value in spark_cfg.get("configs", {}).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(cfg.get("logging", {}).get("level", "WARN"))
    return spark
