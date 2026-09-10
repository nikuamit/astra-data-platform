# Astra Data Platform

> Production-grade batch & streaming data platform — Bronze → Silver → Gold medallion architecture.

[![Python 3.11](https://img.shields.io/badge/python-3.11-3572A5?logo=python&logoColor=white)](https://python.org)
[![PySpark 3.5](https://img.shields.io/badge/pyspark-3.5-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/delta--lake-3.2-00ADD8)](https://delta.io)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Built by **[Amit Kumar Sahu](https://nikuamit.github.io)** — Senior Data Engineer, 9+ years across pharma, SaaS, IoT and enterprise. Mirrors the architecture shipped at **Kenko AI** (500+ multi-tenant clients, 45% query latency cut, 60% fewer data incidents).

---

## Architecture

```
Raw Sources  ──▶  BRONZE (raw, immutable)  ──▶  SILVER (clean, validated)  ──▶  GOLD (analytics-ready)
Kafka / S3        Delta, partitioned             9-rule DQ engine                 Metric marts, SCD2
RDS via DMS       Audit columns added            Quarantine path                  BI / ML ready
```

## Project Structure

```
astra-data-platform/
├── config/
│   ├── platform.yaml             # Spark, storage, Kafka, DQ settings
│   └── entities/events.yaml      # Per-entity schema + DQ rules
├── ingestion/
│   ├── batch/file_to_bronze.py          # CSV/Parquet/JSON → Bronze
│   └── streaming/kafka_to_bronze.py     # Kafka → Bronze (Structured Streaming)
├── processing/
│   ├── silver/bronze_to_silver.py       # Dedup, DQ, Silver write
│   └── gold/silver_to_gold.py           # Aggregations + SCD Type 2
├── dq/
│   ├── models.py            # Pure-Python dataclasses — no PySpark dep
│   ├── quality_engine.py    # 9-rule engine with quarantine writes
│   └── report.py            # HTML DQ report generator
├── utils/
│   ├── spark_session.py     # SparkSession factory
│   ├── config_loader.py     # YAML config with env merge
│   └── logger.py            # Structured JSON logger
├── orchestration/pipeline.py   # Stage runner with retry/backoff
├── scripts/local_run.py        # Full pipeline locally — no AWS needed
└── tests/unit/                 # pytest suite
```

## Quick Start

```bash
git clone https://github.com/nikuamit/astra-data-platform.git
cd astra-data-platform
pip install -r requirements.txt

# Run full pipeline locally (no AWS, no Kafka)
python scripts/local_run.py

# Run tests
pytest tests/unit/ -v

# Batch ingest → Bronze
python -m ingestion.batch.file_to_bronze --source /data/raw/ --entity events --format parquet

# Bronze → Silver (dedup + DQ)
python -m processing.silver.bronze_to_silver --entity events --batch-date 2024-01-01

# Silver → Gold (aggregations)
python -m processing.gold.silver_to_gold --silver-entity events --gold-name events_daily --batch-date 2024-01-01

# Full pipeline with retry
python -m orchestration.pipeline --entity events --batch-date 2024-01-01
```

## Data Quality Engine — 9 Rules

| Rule | Description |
|---|---|
| `not_null` | No nulls in column |
| `unique` | Values must be distinct |
| `range` | Numeric within `[min, max]` |
| `regex` | String matches pattern |
| `accepted_values` | Value in allowed set |
| `referential_integrity` | FK exists in reference |
| `freshness` | Data not older than N hours |
| `schema_check` | Expected columns present |
| `semantic` | Business-logic validation |

Bad rows → quarantine Delta path with `_dq_ts`. HTML report per run.
**Production result at Kenko AI:** 60% fewer incidents, confidence 82% → 97%.

## Config-driven Entity Rules

```yaml
# config/entities/orders.yaml — no code changes needed
entity: orders
dedup_keys: [order_id]
dq_rules:
  - name: order_id_not_null
    type: not_null
    column: order_id
  - name: amount_positive
    type: range
    column: amount
    params: { min: 0, max: 1000000 }
```

## Tech Stack

| Layer | Technology |
|---|---|
| Processing | PySpark 3.5 |
| Storage | Delta Lake 3.2 |
| Streaming | Kafka + Structured Streaming |
| Orchestration | Stage runner / Airflow-ready |
| Config | YAML with env-level deep merge |
| Testing | pytest · ruff |
| Logging | Structured JSON |

## Roadmap
- [ ] CI/CD — GitHub Actions (lint + test + auto-release)
- [ ] Apache Iceberg table format
- [ ] dbt Gold layer models
- [ ] Airflow DAG wrappers
- [ ] Great Expectations integration

## License
MIT — [Amit Kumar Sahu](https://www.linkedin.com/in/aks1993) · [nikuamit.github.io](https://nikuamit.github.io)
