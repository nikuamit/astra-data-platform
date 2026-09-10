# Astra Data Platform

> Production-grade batch & streaming data platform — Bronze → Silver → Gold medallion architecture on AWS.

[![Python 3.11](https://img.shields.io/badge/python-3.11-blue)](https://python.org)
[![PySpark 3.5](https://img.shields.io/badge/pyspark-3.5-orange)](https://spark.apache.org)
[![Delta Lake 3.2](https://img.shields.io/badge/delta--lake-3.2-green)](https://delta.io)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

Built by **[Amit Kumar Sahu](https://nikuamit.github.io)** — Senior Data Engineer, 9+ years.  
Mirrors the architecture shipped at Kenko AI (500+ multi-tenant clients, 45% latency reduction, 60% fewer data incidents).

---

## Architecture

```
Raw Sources (Kafka / S3 / RDS via DMS CDC)
         │
         ▼
┌────────────────────────────────────────┐
│  BRONZE  — raw, immutable              │
│  Kafka stream → Delta                  │
│  CSV / Parquet / JSON batch → Delta    │
└──────────────┬─────────────────────────┘
               │  dedupe · DQ · schema
               ▼
┌────────────────────────────────────────┐
│  SILVER  — clean, validated            │
│  9-rule DQ engine + quarantine         │
│  Dedup via window function             │
│  SCD Type 2 ready                      │
└──────────────┬─────────────────────────┘
               │  aggregations · SCD2
               ▼
┌────────────────────────────────────────┐
│  GOLD  — analytics-ready               │
│  Metric marts · Dimensional models     │
│  BI / ML ready · Delta upsert          │
└────────────────────────────────────────┘
```

---

## Project Structure

```
astra-data-platform/
├── config/
│   ├── platform.yaml              # Spark, storage, Kafka, DQ settings
│   └── entities/events.yaml       # Per-entity schema + DQ rules
├── ingestion/
│   ├── batch/file_to_bronze.py    # CSV/Parquet/JSON → Bronze Delta
│   └── streaming/kafka_to_bronze.py  # Kafka → Bronze (Structured Streaming)
├── processing/
│   ├── silver/bronze_to_silver.py # Cleanse, validate, deduplicate, DQ
│   └── gold/silver_to_gold.py     # Aggregations + SCD Type 2
├── dq/
│   ├── models.py                  # Pure-Python DQ dataclasses (no PySpark dep)
│   ├── quality_engine.py          # Rule engine — quarantine, metrics, alerts
│   └── report.py                  # HTML DQ report generator
├── utils/
│   ├── spark_session.py           # SparkSession factory (config-driven)
│   ├── config_loader.py           # YAML config with env-level deep merge
│   ├── delta_writer.py            # Idempotent Delta writer + merge-upsert
│   └── logger.py                  # Structured JSON logger
├── orchestration/pipeline.py      # Stage runner with retry/backoff (Airflow-ready)
├── scripts/local_run.py           # Full pipeline locally — no AWS needed
├── tests/unit/                    # pytest suite (6 tests, all green)
└── docs/CI_SETUP.md               # CI/CD setup instructions
```

---

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
python -m ingestion.batch.file_to_bronze   --source /data/raw/ --entity events --format parquet

# Bronze → Silver
python -m processing.silver.bronze_to_silver   --entity events --batch-date 2024-01-01

# Silver → Gold
python -m processing.gold.silver_to_gold   --silver-entity events --gold-name events_daily --batch-date 2024-01-01

# Full pipeline
python -m orchestration.pipeline   --entity events --batch-date 2024-01-01
```

---

## Data Quality Engine

6 rule types out of the box — add more in :

| Rule | Description |
|---|---|
|  | Column must have no nulls |
|  | Column values must be distinct |
|  | Numeric column within [min, max] |
|  | String column matches pattern |
|  | Column value in allowed set |
|  | Summary-level count check |

Bad rows → Delta quarantine path with  timestamp. HTML report generated per run.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Processing | PySpark 3.5 |
| Storage format | Delta Lake 3.2 |
| Object storage | AWS S3 (s3a://) |
| Streaming | Apache Kafka + Structured Streaming |
| Orchestration | Pipeline runner / Airflow-ready |
| Config | YAML with env-level deep merge |
| Testing | pytest (6 tests, all green) |
| Linting | ruff |
| CI/CD | See docs/CI_SETUP.md |

---

## Roadmap

- [ ]  — activate via 
- [ ] Airflow DAG wrappers per stage
- [ ] Apache Iceberg table format
- [ ] Schema registry (Confluent / AWS Glue)
- [ ] Great Expectations integration
- [ ] dbt Gold layer models
- [ ] Prometheus metrics emission

---

## Portfolio

Live at **[nikuamit.github.io](https://nikuamit.github.io)**

---

## License

MIT — [Amit Kumar Sahu](https://github.com/nikuamit)
