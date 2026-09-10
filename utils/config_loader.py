"""
utils/config_loader.py
Loads and merges platform config; supports env-level overrides.
"""

from __future__ import annotations

import os
import yaml
from pathlib import Path
from functools import lru_cache


@lru_cache(maxsize=1)
def load_config(path: str | None = None) -> dict:
    base_path = Path(path) if path else Path(__file__).parents[1] / "config" / "platform.yaml"
    with open(base_path) as f:
        cfg = yaml.safe_load(f)

    # Override with env-specific file if present (e.g. config/prod.yaml)
    env = os.getenv("ASTRA_ENV") or cfg.get("platform", {}).get("env", "dev")
    env_file = base_path.parent / f"{env}.yaml"
    if env_file.exists():
        with open(env_file) as f:
            env_cfg = yaml.safe_load(f)
        cfg = _deep_merge(cfg, env_cfg)

    return cfg


def _deep_merge(base: dict, override: dict) -> dict:
    result = dict(base)
    for key, val in override.items():
        if isinstance(val, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = val
    return result


def get(key_path: str, default=None):
    """Dot-notation accessor: get('storage.base_path')"""
    cfg = load_config()
    parts = key_path.split(".")
    node = cfg
    for p in parts:
        if not isinstance(node, dict):
            return default
        node = node.get(p, default)
    return node
