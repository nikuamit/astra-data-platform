"""
scripts/local_run.py
Zero-infra local dev runner.

Spins up a local PySpark session, generates synthetic event data,
and runs the full Bronze → Silver → Gold pipeline to a local /tmp path.

Usage:
    python scripts/local_run.py
    python scripts/local_run.py --rows 5000 --entity events
"""

from __future__ import annotations

import argparse
import random
import sys
import uuid
from datetime import date, timedelta
from pathlib import Path

# Allow running from project root
sys.path.insert(0, str(Path(__file__).parents[1]))

from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F

from utils.logger import get_logger

log = get_logger(__name__, level="INFO")

LOCAL_BASE = "/tmp/astra-local"
EVENT_TYPES = ["page_view", "click", "purchase", "signup", "logout"]


def _make_spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("astra-local-dev")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def _synthetic_events(spark: SparkSession, n: int, batch_date: str) -> object:
    rows = [
        Row(
            event_id=str(uuid.uuid4()),
            user_id=f"u_{random.randint(1, 200)}",
            event_type=random.choice(EVENT_TYPES + [None]),   # inject some nulls
            amount=round(random.uniform(-10, 2000), 2),        # inject out-of-range values
            _ingest_ts=F.current_timestamp(),
            _source_path="/synthetic",
            _batch_date=batch_date,
        )
        for _ in range(n)
    ]
    return spark.createDataFrame(rows)


def run(rows: int = 1000, entity: str = "events") -> None:
    batch_date = str(date.today())
    bronze_path = f"{LOCAL_BASE}/bronze/{entity}"
    silver_path = f"{LOCAL_BASE}/silver/{entity}"
    gold_path   = f"{LOCAL_BASE}/gold/{entity}_daily"
    quarantine  = f"{LOCAL_BASE}/quarantine/{entity}"

    spark = _make_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ── BRONZE ────────────────────────────────────────────────────────────────
    log.info("generating synthetic bronze data", extra={"rows": rows, "entity": entity})
    bronze_df = _synthetic_events(spark, rows, batch_date)
    (
        bronze_df
        .withColumn("_ingest_ts", F.current_timestamp())
        .write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("_batch_date")
        .save(bronze_path)
    )
    log.info("bronze written", extra={"path": bronze_path})

    # ── SILVER ────────────────────────────────────────────────────────────────
    from dq.quality_engine import DQRule, QualityEngine, RuleType

    df = spark.read.format("delta").load(bronze_path)

    # Dedup
    from pyspark.sql.window import Window
    w = Window.partitionBy("event_id").orderBy(F.col("_ingest_ts").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    # DQ
    rules = [
        DQRule("event_id_not_null",   RuleType.NOT_NULL,        column="event_id"),
        DQRule("user_id_not_null",    RuleType.NOT_NULL,        column="user_id"),
        DQRule("event_type_accepted", RuleType.ACCEPTED_VALUES, column="event_type",
               params={"values": EVENT_TYPES}),
        DQRule("amount_range",        RuleType.RANGE,           column="amount",
               params={"min": 0, "max": 1_000_000}),
    ]
    engine = QualityEngine(rules=rules, fail_on_error=False, alert_threshold=0.20)
    clean_df, dq_results = engine.run(df, quarantine_path=quarantine)

    print("\n── DQ Results ─────────────────────────────────────")
    for r in dq_results:
        status = "✅ PASS" if r.passed else "❌ FAIL"
        print(f"  {status}  {r.rule_name:<30}  failed={r.failed_rows}/{r.total_rows}  ({r.failure_rate:.1%})")
    print()

    silver_df = clean_df.withColumn("_silver_ts", F.current_timestamp())
    (
        silver_df.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("_batch_date")
        .save(silver_path)
    )
    log.info("silver written", extra={"path": silver_path, "rows": silver_df.count()})

    # ── GOLD ──────────────────────────────────────────────────────────────────
    gold_df = (
        silver_df
        .groupBy("_batch_date", "event_type")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("user_id").alias("unique_users"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
        )
        .withColumn("_gold_ts", F.current_timestamp())
    )
    (
        gold_df.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .save(gold_path)
    )

    print("── Gold Aggregate ──────────────────────────────────")
    gold_df.orderBy("event_type").show(truncate=False)
    print(f"All layers written under: {LOCAL_BASE}")


def _parse():
    p = argparse.ArgumentParser(description="Astra local dev runner")
    p.add_argument("--rows", type=int, default=1000)
    p.add_argument("--entity", default="events")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse()
    run(args.rows, args.entity)
