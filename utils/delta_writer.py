"""
utils/delta_writer.py
Idempotent Delta Lake writer with merge-upsert support.
"""

from __future__ import annotations

from typing import Sequence

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from utils.logger import get_logger

log = get_logger(__name__)


def write_delta(
    df: DataFrame,
    path: str,
    mode: str = "append",
    partition_by: Sequence[str] | None = None,
    merge_keys: Sequence[str] | None = None,
) -> None:
    """
    Write a DataFrame to Delta Lake.

    Args:
        df:           Source DataFrame.
        path:         Absolute Delta table path (s3a:// or local).
        mode:         'append' | 'overwrite' | 'upsert' (requires merge_keys).
        partition_by: Columns to partition by.
        merge_keys:   Join keys used for upsert mode.
    """
    if mode == "upsert":
        if not merge_keys:
            raise ValueError("merge_keys required for upsert mode")
        _upsert(df, path, list(merge_keys))
        return

    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
    log.info("delta write complete", extra={"path": path, "mode": mode})


def _upsert(df: DataFrame, path: str, merge_keys: list[str]) -> None:
    spark: SparkSession = df.sparkSession
    condition = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)

    if DeltaTable.isDeltaTable(spark, path):
        target = DeltaTable.forPath(spark, path)
        (
            target.alias("t")
            .merge(df.alias("s"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        log.info("delta upsert complete", extra={"path": path, "keys": merge_keys})
    else:
        df.write.format("delta").save(path)
        log.info("delta initial write (upsert → append)", extra={"path": path})
