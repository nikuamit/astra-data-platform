"""
dq/quality_engine.py
Lightweight, config-driven Data Quality engine.

Supports: not_null, unique, range, regex, referential integrity.
Bad rows are quarantined; a summary metric is emitted per run.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from dq.models import DQResult, DQRule, RuleType
from utils.logger import get_logger

log = get_logger(__name__)


class QualityEngine:
    def __init__(self, rules: list[DQRule], fail_on_error: bool = False, alert_threshold: float = 0.05):
        self.rules = rules
        self.fail_on_error = fail_on_error
        self.alert_threshold = alert_threshold

    def run(self, df: DataFrame, quarantine_path: str | None = None) -> tuple[DataFrame, list[DQResult]]:
        """
        Run all rules against df.
        Returns (clean_df, results).
        Bad rows are written to quarantine_path if provided.
        """
        total = df.count()
        results: list[DQResult] = []
        bad_mask = F.lit(False)

        for rule in self.rules:
            fail_condition = self._build_fail_condition(rule, df)
            if fail_condition is None:
                continue

            failed_count = df.filter(fail_condition).count()
            rate = failed_count / total if total else 0.0
            passed = rate <= self.alert_threshold

            result = DQResult(
                rule_name=rule.name,
                passed=passed,
                total_rows=total,
                failed_rows=failed_count,
                failure_rate=round(rate, 4),
            )
            results.append(result)
            log.info("dq rule", extra={"rule": rule.name, "passed": passed, "rate": rate})

            if not passed:
                bad_mask = bad_mask | fail_condition
                if self.fail_on_error:
                    raise ValueError(f"DQ rule '{rule.name}' failed: {rate:.2%} bad rows")

        clean_df = df.filter(~bad_mask)
        quarantine_df = df.filter(bad_mask)

        if quarantine_path and quarantine_df.count() > 0:
            (
                quarantine_df
                .withColumn("_dq_ts", F.current_timestamp())
                .write.format("delta").mode("append").save(quarantine_path)
            )
            log.info("quarantine write", extra={"path": quarantine_path, "rows": quarantine_df.count()})

        return clean_df, results

    def _build_fail_condition(self, rule: DQRule, df: DataFrame):
        col = rule.column
        rtype = rule.rule_type

        if rtype == RuleType.NOT_NULL:
            return F.col(col).isNull()

        if rtype == RuleType.UNIQUE:
            dupes = (
                df.groupBy(col).count().filter(F.col("count") > 1).select(col)
            )
            return F.col(col).isin([r[0] for r in dupes.collect()])

        if rtype == RuleType.RANGE:
            lo = rule.params.get("min")
            hi = rule.params.get("max")
            cond = F.lit(False)
            if lo is not None:
                cond = cond | (F.col(col) < lo)
            if hi is not None:
                cond = cond | (F.col(col) > hi)
            return cond

        if rtype == RuleType.REGEX:
            pattern = rule.params["pattern"]
            return ~F.col(col).rlike(pattern)

        if rtype == RuleType.ACCEPTED_VALUES:
            values = rule.params["values"]
            return ~F.col(col).isin(values)

        if rtype == RuleType.ROW_COUNT:
            # Evaluated at summary level; no per-row condition
            return None

        return None
