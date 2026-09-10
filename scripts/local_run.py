"""
scripts/local_run.py
Run the full Bronze→Silver→Gold pipeline locally using /tmp — no AWS needed.
Generates synthetic events with intentional bad rows to demo DQ.
Usage: python scripts/local_run.py
"""
from __future__ import annotations
import random, os, sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
import pyspark.sql.functions as F
from dq.models import DQRule, RuleType
from dq.quality_engine import QualityEngine
from dq.report import render_html_report

BASE = "/tmp/astra_local"
BRONZE = f"{BASE}/bronze/events"
SILVER = f"{BASE}/silver/events"
GOLD   = f"{BASE}/gold/events_daily"
QUAR   = f"{BASE}/quarantine/events"
REPORT = f"{BASE}/dq_report.html"

def make_spark():
    return (SparkSession.builder.master("local[2]").appName("astra-local")
            .config("spark.sql.shuffle.partitions","4")
            .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())

def synthetic_events(n=1000):
    random.seed(42)
    event_types = ["page_view","click","purchase","signup"]
    rows = []
    base_ts = datetime(2024, 1, 1)
    for i in range(n):
        bad = i % 20 == 0
        rows.append((
            None if bad else f"evt_{i:06d}",          # event_id — intentional nulls
            f"user_{random.randint(1,200):04d}",
            f"tenant_{random.randint(1,10):02d}",
            random.choice(event_types),
            -50.0 if bad else round(random.uniform(0.5, 999.0), 2),  # bad amount
            base_ts + timedelta(seconds=i*60),
            "2024-01-01",
        ))
    return rows

SCHEMA = (StructType()
    .add("event_id",    StringType())
    .add("user_id",     StringType())
    .add("tenant_id",   StringType())
    .add("event_type",  StringType())
    .add("amount",      DoubleType())
    .add("event_ts",    TimestampType())
    .add("_batch_date", StringType()))

def main():
    print("\n=== Astra Local Run ===\n")
    spark = make_spark()
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Generate + write Bronze
    print("1/4  Writing Bronze ...")
    rows = synthetic_events(1000)
    df = spark.createDataFrame(rows, SCHEMA)
    (df.withColumn("_ingest_ts", F.current_timestamp())
       .write.format("delta").mode("overwrite").save(BRONZE))
    print(f"     Bronze: {df.count()} rows → {BRONZE}")

    # 2. Run DQ
    print("2/4  Running Data Quality checks ...")
    bronze_df = spark.read.format("delta").load(BRONZE)
    rules = [
        DQRule("event_id_not_null",    RuleType.NOT_NULL,        column="event_id"),
        DQRule("amount_positive",      RuleType.RANGE,            column="amount", params={"min":0,"max":100000}),
        DQRule("event_type_valid",     RuleType.ACCEPTED_VALUES,  column="event_type",
               params={"values":["page_view","click","purchase","signup","logout"]}),
        DQRule("user_id_not_null",     RuleType.NOT_NULL,        column="user_id"),
    ]
    engine = QualityEngine(rules=rules, alert_threshold=0.10)
    clean_df, results = engine.run(bronze_df, quarantine_path=QUAR)

    print(f"\n     {'Rule':<30} {'Status':<10} {'Failed':>8} {'Rate':>8}")
    print(f"     {'-'*60}")
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"     {r.rule_name:<30} {status:<10} {r.failed_rows:>8,} {r.failure_rate:>7.1%}")

    # 3. Write Silver
    print(f"\n3/4  Writing Silver ({clean_df.count()} clean rows) ...")
    (clean_df.withColumn("_silver_ts", F.current_timestamp())
             .write.format("delta").mode("overwrite").save(SILVER))

    # 4. Build Gold
    print("4/4  Building Gold aggregate ...")
    silver_df = spark.read.format("delta").load(SILVER)
    gold = (silver_df.groupBy("_batch_date","event_type")
            .agg(F.count("*").alias("event_count"),
                 F.sum("amount").alias("total_amount"),
                 F.countDistinct("user_id").alias("unique_users"))
            .withColumn("_gold_ts", F.current_timestamp()))
    gold.write.format("delta").mode("overwrite").save(GOLD)

    print("\n     Gold preview:")
    gold.orderBy("event_type").show(truncate=False)

    # 5. HTML Report
    render_html_report(results, entity="events", batch_date="2024-01-01", output_path=REPORT)
    print(f"\n DQ report saved → {REPORT}")
    print("=== Done ===\n")
    spark.stop()

if __name__ == "__main__":
    main()
