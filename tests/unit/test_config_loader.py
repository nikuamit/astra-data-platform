"""
tests/unit/test_config_loader.py
"""

import pytest
from utils.config_loader import _deep_merge


def test_deep_merge_simple():
    base = {"a": 1, "b": {"c": 2, "d": 3}}
    override = {"b": {"c": 99}}
    result = _deep_merge(base, override)
    assert result["b"]["c"] == 99
    assert result["b"]["d"] == 3  # preserved


def test_deep_merge_new_key():
    base = {"a": 1}
    override = {"b": 2}
    result = _deep_merge(base, override)
    assert result["b"] == 2
    assert result["a"] == 1


def test_deep_merge_overwrite_scalar():
    base = {"a": {"x": 1}}
    override = {"a": "replaced"}
    result = _deep_merge(base, override)
    assert result["a"] == "replaced"
