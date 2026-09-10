"""utils/config_loader.py — YAML config loader with env-level deep merge."""
from __future__ import annotations
import os, yaml
from pathlib import Path
from functools import lru_cache

@lru_cache(maxsize=1)
def load_config(path: str | None = None) -> dict:
    base = Path(path) if path else Path(__file__).parents[1] / "config" / "platform.yaml"
    with open(base) as f:
        cfg = yaml.safe_load(f)
    env = os.getenv("ASTRA_ENV") or cfg.get("platform", {}).get("env", "dev")
    env_file = base.parent / f"{env}.yaml"
    if env_file.exists():
        with open(env_file) as f:
            cfg = _deep_merge(cfg, yaml.safe_load(f))
    return cfg

def _deep_merge(base: dict, override: dict) -> dict:
    result = dict(base)
    for k, v in override.items():
        result[k] = _deep_merge(result[k], v) if isinstance(v, dict) and isinstance(result.get(k), dict) else v
    return result

def get(key_path: str, default=None):
    """Dot-notation accessor: get('storage.base_path')"""
    node = load_config()
    for p in key_path.split("."):
        if not isinstance(node, dict): return default
        node = node.get(p, default)
    return node
