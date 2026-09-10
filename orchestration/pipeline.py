"""
orchestration/pipeline.py
Stage-based pipeline runner with retry + backoff.
Each Stage wraps cleanly as an Airflow PythonOperator.
Usage: python -m orchestration.pipeline --entity events --batch-date 2024-01-01
"""
from __future__ import annotations
import argparse, time
from typing import Callable
from utils.logger import get_logger

log = get_logger(__name__)

class Stage:
    def __init__(self, name: str, fn: Callable, retries: int = 1):
        self.name, self.fn, self.retries = name, fn, retries

    def execute(self, **kwargs) -> None:
        for attempt in range(1, self.retries + 2):
            try:
                t0 = time.time()
                self.fn(**kwargs)
                log.info("stage_ok", extra={"stage": self.name, "elapsed_s": round(time.time()-t0,2)})
                return
            except Exception as e:
                log.error("stage_fail", extra={"stage": self.name, "attempt": attempt, "error": str(e)})
                if attempt > self.retries: raise
                time.sleep(2 ** attempt)

class Pipeline:
    def __init__(self, name: str):
        self.name, self.stages = name, []

    def add(self, stage: Stage) -> "Pipeline":
        self.stages.append(stage); return self

    def run(self, **kwargs) -> None:
        log.info("pipeline_start", extra={"pipeline": self.name})
        t0 = time.time()
        for s in self.stages:
            s.execute(**kwargs)
        log.info("pipeline_done", extra={"pipeline": self.name, "total_s": round(time.time()-t0,2)})

def build_batch_pipeline(entity: str) -> Pipeline:
    from processing.silver.bronze_to_silver import process as to_silver
    from processing.gold.silver_to_gold import build_aggregate
    p = Pipeline(f"{entity}_batch")
    p.add(Stage("bronze_to_silver",
                lambda batch_date, **kw: to_silver(entity, batch_date), retries=2))
    p.add(Stage("silver_to_gold",
                lambda batch_date, **kw: build_aggregate(
                    silver_entity=entity, gold_name=f"{entity}_daily",
                    group_cols=["_batch_date"], agg_exprs={"count": "count(1)"},
                    batch_date=batch_date), retries=1))
    return p

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--entity", required=True)
    ap.add_argument("--batch-date", required=True)
    args = ap.parse_args()
    build_batch_pipeline(args.entity).run(batch_date=args.batch_date)
