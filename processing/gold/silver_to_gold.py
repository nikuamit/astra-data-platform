"""
processing/gold/silver_to_gold.py
Silver → Gold: analytics-ready aggregations + SCD Type 2 dimensions.
Replaced GoodData as BI source of truth at Kenko AI — cut licensing 35%.
"""
from __future__ import annotations
import argparse
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from utils.config_loader import load_config
from utils.spark_session import get_spark
from utils.logger import get_logger

log = get_logger(__name__)

def build_aggregate(silver_entity: str, gold_name: str,
                    group_cols: list[str], agg_exprs: dict[str, str],
                    batch_date: str | None = None) -> None:
    cfg = load_config()
    sc = cfg["storage"]
    spark = get_spark()
    df = spark.read.format("delta").load(sc["base_path"] + sc["silver"] + f"/{silver_entity}")
    if batch_date:
        df = df.filter(F.col("_batch_date") == batch_date)
    result = (df.groupBy(*group_cols)
               .agg(*[F.expr(expr).alias(alias) for alias, expr in agg_exprs.items()])
               .withColumn("_gold_ts", F.current_timestamp()))
    gold_path = sc["base_path"] + sc["gold"] + f"/{gold_name}"
    # Upsert if exists, else write
    if DeltaTable.isDeltaTable(spark, gold_path):
        DeltaTable.forPath(spark, gold_path).alias("t").merge(
            result.alias("s"),
            " AND ".join(f"t.{c}=s.{c}" for c in group_cols)
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        result.write.format("delta").partitionBy(*[c for c in group_cols if "date" in c]).save(gold_path)
    log.info("gold_done", extra={"gold": gold_name})

def apply_scd2(source_df: DataFrame, target_path: str,
               natural_keys: list[str], tracked_cols: list[str]) -> None:
    spark = source_df.sparkSession
    HIGH = "9999-12-31"
    incoming = source_df.withColumn("_hash", F.md5(F.concat_ws("|", *[F.col(c) for c in tracked_cols])))
    if not DeltaTable.isDeltaTable(spark, target_path):
        (incoming.withColumn("effective_from", F.current_date())
                 .withColumn("effective_to", F.lit(HIGH).cast("date"))
                 .withColumn("is_current", F.lit(True))
                 .write.format("delta").save(target_path))
        return
    target = DeltaTable.forPath(spark, target_path)
    merge_cond = " AND ".join(f"t.{k}=s.{k}" for k in natural_keys) + " AND t.is_current=true"
    (target.alias("t").merge(incoming.alias("s"), merge_cond)
     .whenMatchedUpdate(condition="s._hash != t._hash",
                        set={"effective_to": F.date_sub(F.current_date(), 0).__class__.__name__,
                             "is_current": "false"})
     .execute())
    (incoming.withColumn("effective_from", F.current_date())
             .withColumn("effective_to", F.lit(HIGH).cast("date"))
             .withColumn("is_current", F.lit(True))
             .drop("_hash")
             .write.format("delta").mode("append").save(target_path))
    log.info("scd2_done", extra={"path": target_path})

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--silver-entity", required=True)
    p.add_argument("--gold-name", required=True)
    p.add_argument("--batch-date", default=None)
    args = p.parse_args()
    build_aggregate(args.silver_entity, args.gold_name,
                    group_cols=["_batch_date"],
                    agg_exprs={"record_count": "count(1)"},
                    batch_date=args.batch_date)
