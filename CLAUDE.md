# CLAUDE.md — Astra Data Platform

This file tells Claude Code exactly how this repo is structured and why,
so any session (terminal or otherwise) picks up context immediately.

## What this project is

Astra is a **portfolio-grade, production-pattern data platform** built by Amit Sahu (Senior Data Engineer, 9+ years).
The primary purpose is **dual**: it is a real, runnable data platform AND a public demonstration of
engineering maturity for recruiters and hiring managers reviewing the GitHub profile.

Every design decision is intentional and should remain visible in the code:
- Config-driven (not hardcoded): shows enterprise readiness
- Medallion architecture (Bronze/Silver/Gold): standard in modern data orgs
- DQ as a first-class concern: shows production thinking, not just happy-path
- Structured logging everywhere: shows ops awareness
- Retry/backoff in pipeline runner: shows failure-mode thinking
- CI/CD with auto-release: shows DevOps maturity

## Key conventions — follow these when editing

1. **No hardcoded values.** Everything goes through `utils/config_loader.py` → `config/platform.yaml`.
2. **Always use `utils/logger.py`** (`get_logger(__name__)`). Never `print()`.
3. **Delta writes go through `utils/delta_writer.py`**, not raw `.write.format("delta")` calls.
4. **Every new module gets a docstring** explaining: what it does, design decisions made, and why.
5. **New entity types** get a `config/entities/<entity>.yaml` — never inline schema/rules in Python.
6. **Tests live in `tests/unit/`**. New DQ rules, utils, or processors need a matching test.
7. **Keep imports clean**: stdlib → third-party → internal (ruff enforces this).

## Running locally

```bash
# Install
pip install -r requirements.txt

# Unit tests (no Spark cluster needed — uses local[1])
pytest tests/unit/ -v

# Full pipeline dry-run (needs local Delta paths — adjust config/platform.yaml base_path)
python -m orchestration.pipeline --entity events --batch-date 2024-01-01
```

## What Claude Code should help with

- Adding new entity configs (`config/entities/`)
- Writing integration tests (`tests/integration/`)
- Implementing Roadmap items (Airflow DAGs, CDC ingestion, dbt Gold models)
- Fixing bugs found by `pytest` or `ruff`
- Committing and pushing changes

## What NOT to change

- The Bronze layer stores **raw, untransformed data only** — no business logic there, ever.
- The DQ engine's `quarantine_path` write must remain — it's the audit trail.
- The `_ingest_ts`, `_source_path`, `_batch_date` audit columns are sacred — they enable lineage.
- Do not collapse Bronze/Silver into a single step even if it seems simpler — the separation is the design.

## Repo owner

Amit Sahu — nikuamit on GitHub. Bengaluru, India.
