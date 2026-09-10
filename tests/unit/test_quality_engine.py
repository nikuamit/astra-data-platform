"""tests/unit/test_quality_engine.py"""
import pytest
from pyspark.sql import SparkSession
from dq.models import DQRule, RuleType
from dq.quality_engine import QualityEngine

@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder.master("local[1]").appName("astra-test")
            .config("spark.sql.shuffle.partitions","2").getOrCreate())

def test_not_null_passes(spark):
    df = spark.createDataFrame([("a",),("b",)], ["name"])
    rule = DQRule("name_nn", RuleType.NOT_NULL, column="name")
    clean, results = QualityEngine([rule]).run(df)
    assert results[0].passed and clean.count() == 2

def test_not_null_catches_nulls(spark):
    df = spark.createDataFrame([("a",),(None,)], ["name"])
    rule = DQRule("name_nn", RuleType.NOT_NULL, column="name")
    clean, results = QualityEngine([rule], alert_threshold=0.0).run(df)
    assert not results[0].passed and results[0].failed_rows == 1 and clean.count() == 1

def test_range_rule(spark):
    df = spark.createDataFrame([(1,),(5,),(200,)], ["age"])
    rule = DQRule("age_range", RuleType.RANGE, column="age", params={"min":0,"max":150})
    clean, results = QualityEngine([rule], alert_threshold=0.0).run(df)
    assert results[0].failed_rows == 1 and clean.count() == 2

def test_accepted_values(spark):
    df = spark.createDataFrame([("active",),("inactive",),("zombie",)], ["status"])
    rule = DQRule("valid_status", RuleType.ACCEPTED_VALUES, column="status",
                  params={"values":["active","inactive"]})
    clean, results = QualityEngine([rule], alert_threshold=0.0).run(df)
    assert results[0].failed_rows == 1 and clean.count() == 2

def test_fail_on_error_raises(spark):
    df = spark.createDataFrame([(None,),(None,)], ["col"])
    rule = DQRule("col_nn", RuleType.NOT_NULL, column="col")
    with pytest.raises(ValueError, match="DQ rule"):
        QualityEngine([rule], fail_on_error=True, alert_threshold=0.0).run(df)

def test_multiple_rules(spark):
    df = spark.createDataFrame([("a",1),("b",200),(None,50)], ["name","age"])
    rules = [
        DQRule("name_nn",  RuleType.NOT_NULL, column="name"),
        DQRule("age_range", RuleType.RANGE,   column="age", params={"min":0,"max":150}),
    ]
    clean, results = QualityEngine(rules, alert_threshold=0.0).run(df)
    assert len(results) == 2
    assert all(not r.passed for r in results)
