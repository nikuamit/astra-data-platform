"""
processing/gold/silver_to_gold.py
Silver → Gold: produce analytics-ready aggregations and SCD Type 2 dimensions.

Two modes:
  - aggregate: build a metric mart (e.g. daily active users, revenue by entity)
  - scd2:      maintain a Slowly Changing Dimension Type 2 table
"""

from __future__ import annotations

import argparse
from typing import Literal

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from utils.config_loader import load_config
from utils.delta_writer import write_delta
from utils.logger import get_logger
from utils.spark_session import get_spark

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Aggregation mart
# ---------------------------------------------------------------------------

def build_aggregate(
    silver_entity: str,
    gold_name: str,
    group_cols: list[str],
    agg_exprs: dict[str, str],
    batch_date: str | None = None,
) -> None:
    """
    Generic aggregation builder.

    Args:
        silver_entity: Source silver table name.
        gold_name:     Target gold table name.
        group_cols:    Columns to group by.
        agg_exprs:     Dict of {alias: "spark_agg_expr"}.
        batch_date:    Optional partition filter (YYYY-MM-DD).
    """
    cfg = load_config()
    storage = cfg["storage"]
    silver_path = storage["base_path"] + storage["silver"] + f"/{silver_entity}"
    gold_path = storage["base_path"] + storage["gold"] + f"/{gold_name}"

    spark = get_spark()
    df = spark.read.format("delta").load(silver_path)

    if batch_date:
        df = df.filter(F.col("_batch_date") == batch_date)

    aggs = [F.expr(expr).alias(alias) for alias, expr in agg_exprs.items()]
    result = df.groupBy(*group_cols).agg(*aggs)
    result = result.withColumn("_gold_ts", F.current_timestamp())

    write_delta(result, gold_path, mode="append", partition_by=["_batch_date"] if "_batch_date" in group_cols else None)
    log.info("gold aggregate written", extra={"gold": gold_name, "path": gold_path})


# ---------------------------------------------------------------------------
# SCD Type 2
# ---------------------------------------------------------------------------

def apply_scd2(
    source_df: DataFrame,
    target_path: str,
    natural_keys: list[str],
    tracked_cols: list[str],
    effective_col: str = "effective_from",
    expiry_col: str = "effective_to",
    current_col: str = "is_current",
) -> None:
    """
    Merge source_df into a SCD Type 2 Delta table.
    Closes old records and inserts new ones when tracked_cols change.
    """
    spark = source_df.sparkSession
    high_date = "9999-12-31"

    # Annotate source
    incoming = source_df.withColumn("_row_hash", F.md5(F.concat_ws("|", *[F.col(c) for c in tracked_cols])))

    if not DeltaTable.isDeltaTable(spark, target_path):
        # Initial load
        initial = (
            incoming
            .withColumn(effective_col, F.current_date())
            .withColumn(expiry_col, F.lit(high_date).cast("date"))
            .withColumn(current_col, F.lit(True))
        )
        initial.write.format("delta").save(target_path)
        log.info("scd2 initial load", extra={"path": target_path})
        return

    target = DeltaTable.forPath(spark, target_path)
    target_df = target.toDF()

    join_cond = " AND ".join(f"t.{k} = s.{k}" for k in natural_keys)

    # Detect changed rows
    changed = (
        incoming.alias("s")
        .join(target_df.filter(F.col(current_col)).alias("t"), natural_keys, "left")
        .filter(
            F.col(f"t.{natural_keys[0]}").isNull() |  # new record
            (F.col("s._row_hash") != F.col("t._row_hash"))  # changed
        )
    )

    # Close existing current records
    close_condition = join_cond + f" AND t.{current_col} = true AND s._row_hash != t._row_hash"
    (
        target.alias("t")
        .merge(changed.alias("s"), close_condition)
        .whenMatchedUpdate(set={
            expiry_col: F.date_sub(F.current_date(), 0).__class__.__name__,  # today
            current_col: "false",
        })
        .execute()
    )

    # Insert new/changed records
    new_rows = (
        changed
        .withColumn(effective_col, F.current_date())
        .withColumn(expiry_col, F.lit(high_date).cast("date"))
        .withColumn(current_col, F.lit(True))
        .drop("_row_hash")
    )
    new_rows.write.format("delta").mode("append").save(target_path)
    log.info("scd2 merge complete", extra={"path": target_path, "new_rows": new_rows.count()})


def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["aggregate", "scd2"], required=True)
    p.add_argument("--silver-entity", required=True)
    p.add_argument("--gold-name", required=True)
    p.add_argument("--batch-date", default=None)
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if args.mode == "aggregate":
        # Example: daily event counts — override agg_exprs in your own driver
        build_aggregate(
            silver_entity=args.silver_entity,
            gold_name=args.gold_name,
            group_cols=["_batch_date", "event_type"],
            agg_exprs={"event_count": "count(1)", "unique_users": "count(distinct user_id)"},
            batch_date=args.batch_date,
        )
