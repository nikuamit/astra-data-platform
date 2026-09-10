"""dq/models.py — Pure-Python DQ dataclasses. No PySpark dependency — importable anywhere."""
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

class RuleType(str, Enum):
    NOT_NULL        = "not_null"
    UNIQUE          = "unique"
    RANGE           = "range"
    REGEX           = "regex"
    ACCEPTED_VALUES = "accepted_values"
    REFERENTIAL     = "referential_integrity"
    FRESHNESS       = "freshness"
    SCHEMA          = "schema_check"
    SEMANTIC        = "semantic"

@dataclass
class DQRule:
    name:      str
    rule_type: RuleType
    column:    str | None = None
    params:    dict[str, Any] = field(default_factory=dict)

@dataclass
class DQResult:
    rule_name:    str
    passed:       bool
    total_rows:   int
    failed_rows:  int
    failure_rate: float
    details:      str = ""
