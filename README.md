# Astra Data Platform

> Production-grade batch & streaming data platform — Bronze → Silver → Gold medallion architecture on AWS + Apache Iceberg.

[![CI](https://github.com/nikuamit/astra-data-platform/actions/workflows/ci.yaml/badge.svg)](https://github.com/nikuamit/astra-data-platform/actions)
![Python 3.11](https://img.shields.io/badge/python-3.11-blue)
![PySpark 3.5](https://img.shields.io/badge/pyspark-3.5-orange)
![Delta Lake 3.2](https://img.shields.io/badge/delta--lake-3.2-green)

Built by [Amit Kumar Sahu](https://nikuamit.github.io) — Senior Data Engineer with 9+ years across pharma, SaaS, IoT, and enterprise. Mirrors the architecture shipped at Kenko AI (500+ multi-tenant clients, 45% query latency reduction, 60% fewer data incidents).

---

## Architecture

```
Raw Sources (Kafka / S3 / RDS via DMS CDC)
         │
         ▼
┌────────────────────────────────────┐
│  BRONZE  — raw, immutable          │
│  Kafka stream → Delta              │
│  CSV/Parquet/JSON batch → Delta    │
└──────────────┬─────────────────────┘
               │  dedupe · DQ · schema
               ▼
┌────────────────────────────────────┐
│  SILVER  — clean, validated        │
│  9-check DQ engine                 │
│  Bad rows → quarantine             │
│  SCD Type 2 ready                  │
└──────────────┬─────────────────────┘
               │  aggregations · SCD2
               ▼
┌────────────────────────────────────┐
│  GOLD  — analytics-ready           │
│  Metric marts · Dimensional models │
│  BI / ML ready                     │
└────────────────────────────────────┘
```

## Project Structure

```
astra-data-platform/
├── config/
│   ├── platform.yaml           # Spark, storage, Kafka, DQ settings
│   └── entities/events.yaml    # Per-entity schema + DQ rules
├── ingestion/
│   ├── batch/file_to_bronze.py         # CSV/Parquet/JSON → Bronze
│   └── streaming/kafka_to_bronze.py    # Kafka → Bronze (Structured Streaming)
├── processing/
│   ├── silver/bronze_to_silver.py      # Cleanse, validate, deduplicate
│   └── gold/silver_to_gold.py          # Aggregations + SCD Type 2
├── dq/
│   ├── models.py           # Pure-Python DQ dataclasses (no PySpark dep)
│   ├── quality_engine.py   # 9-rule DQ engine with quarantine
│   └── report.py           # HTML DQ report generator
├── utils/
│   ├── spark_session.py    # SparkSession factory
│   ├── config_loader.py    # YAML config with env overrides
│   └── logger.py           # Structured JSON logger
├── orchestration/pipeline.py   # Stage runner with retry/backoff (Airflow-ready)
├── scripts/local_run.py        # Run full pipeline locally — no AWS needed
├── tests/unit/                 # pytest suite (6 tests, all green)
└── .github/workflows/ci.yaml   # Lint + test + auto-release on main
```

## Quick Start

```bash
git clone https://github.com/nikuamit/astra-data-platform.git
cd astra-data-platform
pip install -r requirements.txt

# Run full pipeline locally (no AWS, no Kafka needed)
python scripts/local_run.py

# Run tests
pytest tests/unit/ -v

# Batch ingest
python -m ingestion.batch.file_to_bronze --source /data/raw/ --entity events --format parquet

# Bronze → Silver
python -m processing.silver.bronze_to_silver --entity events --batch-date 2024-01-01

# Silver → Gold
python -m processing.gold.silver_to_gold --silver-entity events --gold-name events_daily --batch-date 2024-01-01

# Full pipeline
python -m orchestration.pipeline --entity events --batch-date 2024-01-01
```

## Data Quality Engine

9 rule types out of the box:

| Rule | Description |
|---|---|
| `not_null` | Column must have no nulls |
| `unique` | Column values must be distinct |
| `range` | Numeric column within [min, max] |
| `regex` | String column matches pattern |
| `accepted_values` | Column value in allowed set |
| `referential_integrity` | FK exists in reference table |
| `freshness` | Data not older than N hours |
| `schema_check` | Expected columns present |
| `semantic` | Business-logic validation |

Bad rows → quarantine Delta path with `_dq_ts`. HTML report generated per run.

## Tech Stack

| Layer | Technology |
|---|---|
| Processing | PySpark 3.5 |
| Storage format | Delta Lake 3.2 |
| Object storage | AWS S3 |
| Streaming | Apache Kafka + Structured Streaming |
| Orchestration | Pipeline runner / Airflow-ready |
| Config | YAML with env-level deep merge |
| Testing | pytest (6 tests, all green) |
| Linting | ruff |
| CI/CD | GitHub Actions — auto semver tag + release |

## Roadmap
- [ ] Airflow DAG wrappers per stage
- [ ] Apache Iceberg table format (replacing Delta)
- [ ] Schema registry (Confluent / AWS Glue)
- [ ] Prometheus metrics emission
- [ ] dbt Gold layer models
- [ ] Great Expectations integration

## License
MIT — [Amit Kumar Sahu](https://www.linkedin.com/in/aks1993)
