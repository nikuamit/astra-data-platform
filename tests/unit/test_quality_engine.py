"""
tests/unit/test_quality_engine.py
"""

import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from dq.quality_engine import DQRule, QualityEngine, RuleType


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("astra-test")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def test_not_null_passes_clean_data(spark):
    df = spark.createDataFrame([("a",), ("b",)], ["name"])
    rule = DQRule(name="name_not_null", rule_type=RuleType.NOT_NULL, column="name")
    engine = QualityEngine(rules=[rule])
    clean, results = engine.run(df)
    assert results[0].passed
    assert clean.count() == 2


def test_not_null_catches_nulls(spark):
    df = spark.createDataFrame([("a",), (None,)], ["name"])
    rule = DQRule(name="name_not_null", rule_type=RuleType.NOT_NULL, column="name")
    engine = QualityEngine(rules=[rule], alert_threshold=0.0)
    clean, results = engine.run(df)
    assert not results[0].passed
    assert results[0].failed_rows == 1
    assert clean.count() == 1


def test_range_rule(spark):
    df = spark.createDataFrame([(1,), (5,), (200,)], ["age"])
    rule = DQRule(name="age_range", rule_type=RuleType.RANGE, column="age", params={"min": 0, "max": 150})
    engine = QualityEngine(rules=[rule], alert_threshold=0.0)
    clean, results = engine.run(df)
    assert results[0].failed_rows == 1
    assert clean.count() == 2


def test_accepted_values_rule(spark):
    df = spark.createDataFrame([("active",), ("inactive",), ("zombie",)], ["status"])
    rule = DQRule(
        name="valid_status",
        rule_type=RuleType.ACCEPTED_VALUES,
        column="status",
        params={"values": ["active", "inactive"]},
    )
    engine = QualityEngine(rules=[rule], alert_threshold=0.0)
    clean, results = engine.run(df)
    assert results[0].failed_rows == 1
    assert clean.count() == 2


def test_fail_on_error_raises(spark):
    df = spark.createDataFrame([(None,), (None,)], ["col"])
    rule = DQRule(name="col_not_null", rule_type=RuleType.NOT_NULL, column="col")
    engine = QualityEngine(rules=[rule], fail_on_error=True, alert_threshold=0.0)
    with pytest.raises(ValueError, match="DQ rule"):
        engine.run(df)
