# Astra Data Platform

> A production-grade batch & streaming data platform built with PySpark and Delta Lake.

[![CI](https://github.com/nikuamit/astra-data-platform/actions/workflows/ci.yaml/badge.svg)](https://github.com/nikuamit/astra-data-platform/actions)
![Python 3.11](https://img.shields.io/badge/python-3.11-blue)
![PySpark 3.5](https://img.shields.io/badge/pyspark-3.5-orange)
![Delta Lake 3.2](https://img.shields.io/badge/delta--lake-3.2-green)

---

## Overview

Astra is a config-driven data platform that implements the **Bronze → Silver → Gold** medallion architecture for both batch and real-time pipelines. It is designed to run on AWS (S3 + EMR / Databricks) but is fully local-runnable for development.

```
Raw sources
    │
    ▼
┌─────────────────────────────────────────────┐
│  BRONZE  (raw, immutable, partitioned)      │
│  • Kafka stream → Delta                     │
│  • CSV/Parquet/JSON batch → Delta           │
└──────────────────┬──────────────────────────┘
                   │ DQ + dedupe + type-casting
                   ▼
┌─────────────────────────────────────────────┐
│  SILVER  (clean, validated, structured)     │
│  • Schema enforcement                       │
│  • DQ quarantine                            │
│  • SCD Type 2 ready                         │
└──────────────────┬──────────────────────────┘
                   │ aggregations + SCD2 dims
                   ▼
┌─────────────────────────────────────────────┐
│  GOLD  (analytics-ready)                    │
│  • Metric marts                             │
│  • Dimensional models                       │
│  • BI / ML ready                            │
└─────────────────────────────────────────────┘
```

---

## Project Structure

```
astra-data-platform/
├── config/
│   ├── platform.yaml          # Spark, storage, Kafka, DQ settings
│   └── entities/              # Per-entity schema + DQ rules (e.g. events.yaml)
├── ingestion/
│   ├── batch/
│   │   └── file_to_bronze.py  # CSV/Parquet/JSON → Bronze
│   └── streaming/
│       └── kafka_to_bronze.py # Kafka → Bronze (Structured Streaming)
├── processing/
│   ├── bronze/                # (reserved for bronze-level transforms)
│   ├── silver/
│   │   └── bronze_to_silver.py  # Cleanse, validate, deduplicate
│   └── gold/
│       └── silver_to_gold.py    # Aggregations + SCD Type 2
├── dq/
│   └── quality_engine.py      # Rule-based DQ framework
├── utils/
│   ├── spark_session.py       # SparkSession factory
│   ├── config_loader.py       # YAML config with env overrides
│   ├── delta_writer.py        # Idempotent Delta writer + merge-upsert
│   └── logger.py              # Structured JSON logger
├── orchestration/
│   └── pipeline.py            # Stage-based pipeline runner (Airflow-ready)
├── tests/
│   ├── unit/
│   │   ├── test_quality_engine.py
│   │   └── test_config_loader.py
│   └── integration/           # (wire up with real Delta paths)
├── .github/
│   └── workflows/
│       └── ci.yaml            # Test + auto-release on merge to main
├── requirements.txt
└── pyproject.toml
```

---

## Quick Start

```bash
# 1. Clone
git clone https://github.com/nikuamit/astra-data-platform.git
cd astra-data-platform

# 2. Install
pip install -r requirements.txt

# 3. Run tests
pytest tests/unit/ -v

# 4. Batch ingest (local)
python -m ingestion.batch.file_to_bronze \
  --source /data/raw/events/ \
  --entity events \
  --format parquet

# 5. Process Bronze → Silver
python -m processing.silver.bronze_to_silver \
  --entity events \
  --batch-date 2024-01-01

# 6. Build Gold aggregates
python -m processing.gold.silver_to_gold \
  --mode aggregate \
  --silver-entity events \
  --gold-name events_daily \
  --batch-date 2024-01-01

# 7. Run full pipeline
python -m orchestration.pipeline \
  --entity events \
  --batch-date 2024-01-01
```

---

## Configuration

All settings live in `config/platform.yaml`. Override per-environment by setting `ASTRA_ENV=prod` and placing a `config/prod.yaml` with only the keys you want to override (deep merge applied).

**Entity-level config** (`config/entities/events.yaml`) controls DQ rules, dedup keys, and JSON schema for each dataset.

---

## Data Quality

`dq/quality_engine.py` supports the following rule types out of the box:

| Rule | Description |
|---|---|
| `not_null` | Column must have no nulls |
| `unique` | Column values must be distinct |
| `range` | Numeric column within [min, max] |
| `regex` | String column matches pattern |
| `accepted_values` | Column value in allowed set |
| `row_count` | Summary-level count check |

Bad rows are written to the **quarantine** Delta path with a `_dq_ts` timestamp for investigation.

---

## CI / CD

GitHub Actions runs on every PR and `main` push:
1. Lint with **ruff**
2. Unit tests with **pytest**
3. On merge to `main`: automatic **semver tag** + **GitHub Release**

---

## Roadmap

- [ ] CDC ingestion via Debezium + Kafka Connect
- [ ] Schema registry integration (Confluent / AWS Glue)
- [ ] Airflow DAG wrappers for each pipeline stage
- [ ] Metrics emission (Prometheus / CloudWatch)
- [ ] Great Expectations integration for richer DQ
- [ ] dbt Gold layer models

---

## Tech Stack

| Layer | Technology |
|---|---|
| Processing | PySpark 3.5 |
| Storage format | Delta Lake 3.2 |
| Object storage | AWS S3 (s3a://) |
| Streaming source | Apache Kafka |
| Orchestration | Custom pipeline runner / Airflow |
| Config | YAML (env-merged) |
| Testing | pytest |
| Linting | ruff |
| CI/CD | GitHub Actions |

---

## License

MIT
