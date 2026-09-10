"""
orchestration/pipeline.py
Lightweight pipeline runner — chains batch stages with retry + timing.
Drop-in with Airflow/Prefect: each stage can also be wrapped as a Task.

Usage (standalone):
    python -m orchestration.pipeline --entity events --batch-date 2024-01-01
"""

from __future__ import annotations

import argparse
import time
from typing import Callable

from utils.logger import get_logger

log = get_logger(__name__)


class Stage:
    def __init__(self, name: str, fn: Callable, retries: int = 1):
        self.name = name
        self.fn = fn
        self.retries = retries

    def execute(self, **kwargs) -> None:
        for attempt in range(1, self.retries + 2):
            try:
                t0 = time.time()
                self.fn(**kwargs)
                elapsed = round(time.time() - t0, 2)
                log.info("stage complete", extra={"stage": self.name, "elapsed_s": elapsed})
                return
            except Exception as exc:
                log.error("stage failed", extra={"stage": self.name, "attempt": attempt, "error": str(exc)})
                if attempt > self.retries:
                    raise
                time.sleep(2 ** attempt)


class Pipeline:
    def __init__(self, name: str):
        self.name = name
        self.stages: list[Stage] = []

    def add(self, stage: Stage) -> "Pipeline":
        self.stages.append(stage)
        return self

    def run(self, **kwargs) -> None:
        log.info("pipeline start", extra={"pipeline": self.name, "stages": [s.name for s in self.stages]})
        t0 = time.time()
        for stage in self.stages:
            stage.execute(**kwargs)
        log.info("pipeline complete", extra={"pipeline": self.name, "total_s": round(time.time() - t0, 2)})


def build_batch_pipeline(entity: str) -> Pipeline:
    from ingestion.batch.file_to_bronze import ingest
    from processing.silver.bronze_to_silver import process as to_silver
    from processing.gold.silver_to_gold import build_aggregate

    pipeline = Pipeline(name=f"{entity}_batch")

    pipeline.add(Stage("ingest_bronze", lambda batch_date, **kw: None, retries=2))  # source-specific override
    pipeline.add(Stage("bronze_to_silver", lambda batch_date, **kw: to_silver(entity, batch_date), retries=1))
    pipeline.add(Stage(
        "silver_to_gold",
        lambda batch_date, **kw: build_aggregate(
            silver_entity=entity,
            gold_name=f"{entity}_daily",
            group_cols=["_batch_date"],
            agg_exprs={"record_count": "count(1)"},
            batch_date=batch_date,
        ),
        retries=1,
    ))

    return pipeline


def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--entity", required=True)
    p.add_argument("--batch-date", required=True)
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    pipeline = build_batch_pipeline(args.entity)
    pipeline.run(batch_date=args.batch_date)
