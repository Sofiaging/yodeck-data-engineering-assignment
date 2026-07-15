# Yodeck – Senior Data Engineer Take Home Assignment

## Overview

This repository implements a fault-tolerant batch ingestion pipeline that loads daily CSV snapshots from Amazon S3 into a PostgreSQL data warehouse using **Python**, **dlt**, and **dbt**.

The solution focuses on reliable ingestion, schema evolution handling, data quality validation, ingestion monitoring, and reproducible transformations. The raw layer preserves historical snapshots, while dbt provides a clean staging layer with automated testing.

---

# Solution Architecture

```
Amazon S3
    │
    ▼
Python 3.12
(load_s3_csvs.py)

    │
    ├── boto3
    ├── dlt
    ├── validation layer
    └── ingestion logging

    ▼

PostgreSQL

raw schema
    │
    ▼

dbt Core

staging models
    │
    ├── generic tests
    ├── custom tests
    └── monitoring

    ▼

analytics-ready models
```

---

# Repository Structure

```
yodeck-assignment/

├── config/
│   ├── pipeline_config.yml
│   └── tables.yml
│
├── ingestion/
│   ├── load_s3_csvs.py
│   ├── s3_utils.py
│   ├── validators.py
│   └── dlt_db_utils.py
│
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   ├── tests/
│   ├── macros/
│   ├── dbt_project.yml
│   └── profiles.yml (local only)
│
├── requirements.txt
└── README.md
```

---

# Features Implemented

## Ingestion

### Three ingestion modes

* **all**

  * Processes every valid snapshot file found in the S3 bucket.
  * Before loading each snapshot, existing records for the corresponding table and snapshot date are deleted from the raw layer.
  * Guarantees idempotent full reloads.

* **range**

  * Processes only snapshot files whose snapshot date falls within the specified date range.
  * Existing records for the selected snapshot dates are deleted before reloading.
  * Supports safe historical backfills and reprocessing.

* **new**

  * Processes only files that have not previously been ingested.
  * Files are identified using their filename and SHA-256 checksum recorded in `raw.ingestion_log`.
  * Existing data is never deleted; only newly discovered snapshots are ingested.

---

## Data Quality & Validation

Before loading data into PostgreSQL, the ingestion framework performs the following validations:

* **Automatic duplicate snapshot detection** – If multiple files exist for the same table and snapshot date (e.g. a resend), only the most recently uploaded S3 object is ingested.

* **Snapshot completeness validation** – A business snapshot is considered complete only if all expected source tables are present for a given snapshot date. If any table is missing, the entire snapshot date is skipped.

* **Filename validation** – Source filenames must follow the naming convention defined in `config/pipeline_config.yml`.

* **Datatype validation** – Source columns are validated against the expected schema defined in `config/tables.yml`.

* **Primary key validation** – Primary key columns defined in `config/tables.yml` are validated for existence and uniqueness within each individual snapshot.

* **Schema evolution detection** – Newly added or missing source columns are detected before ingestion.

---

## Metadata

Every raw table receives ingestion metadata columns:

* `_source_file`
* `_snapshot_date`
* `_ingested_at`
* `_checksum`

Additionally, every processed file is recorded in `raw.ingestion_log` with:

* file name
* table name
* snapshot date
* row count
* checksum
* execution timestamps
* execution status
* error message
* schema change flag
* ingestion mode
* replaced rows
* dlt load id

---

# dbt Layer

The dbt project provides a clean staging layer built on top of the raw schema.

Implemented models:

* `stg_accounts`
* `stg_subscriptions`
* `stg_exchange_rates`
* `stg_subscription_changes`
* `metrics`

Implemented tests:

### Generic dbt tests

* NOT NULL validations

### Custom tests

* Snapshot uniqueness validation (`primary_key + snapshot_date`)
* Row count anomaly detection between snapshots

---

# Schema Evolution

Schema evolution is handled during ingestion rather than inside dbt.

When a schema change is detected:

* the affected file is identified
* missing columns are automatically created with NULL values
* new columns are detected and reported
* the event is logged in `raw.ingestion_log`
* downstream dbt models continue operating without modification

---

# Design Decisions

### Why dlt?

dlt provides:

* reliable PostgreSQL loading
* built-in metadata management
* simple incremental loading
* minimal boilerplate code

making it well suited for a lightweight ingestion framework.

---

### Why handle schema changes during ingestion?

Schema evolution is addressed before data reaches the warehouse.

This keeps the raw layer resilient while allowing dbt models to remain simple and focused purely on transformations.

---

### Why skip incomplete snapshots?

Business entities across the four datasets belong to the same daily snapshot.

Loading only part of a day's data would produce inconsistent analytics, so incomplete snapshot dates are skipped entirely.

---

# Assumptions

The implementation assumes:

* daily full snapshots
* one snapshot per table per day
* snapshots are immutable
* S3 acts as the landing zone
* PostgreSQL stores historical raw snapshots
* dbt performs transformations only

---

# Pipeline Orchestration

For the assignment the pipeline is executed manually.

In production, orchestration could be implemented using:

* GitHub Actions
* Dagster
* Apache Airflow
* Prefect

A scheduled workflow would:

1. Execute the ingestion pipeline
2. Validate the raw layer
3. Execute `dbt build`
4. Notify on failures or SLA breaches

---

# Scaling Considerations

The solution can be extended for larger workloads by:

* parallel S3 file processing
* PostgreSQL partitioning by snapshot date
* COPY-based bulk loading
* incremental dbt models
* event-driven ingestion using S3 notifications
* orchestration with Airflow or Dagster
* migration to Snowflake, BigQuery or Amazon Redshift

---

# Freshness SLA

Expected arrival:

* one complete business snapshot every 24 hours

SLA:

* all four snapshot files should arrive before **08:00 UTC** each day.

### Blocking issues

* missing daily snapshot
* duplicate primary keys
* invalid datatypes
* invalid filenames

### Alert-only issues

* schema evolution
* row count anomalies
* freshness SLA breaches

---

# Running the Project

## 1. Clone the repository

```bash
git clone <repository-url>
cd yodeck-assignment
```

---

## 2. Create a virtual environment

```bash
python3.12 -m venv .venv

source .venv/bin/activate

pip install -r requirements.txt
```

---

## 3. Configure PostgreSQL

Create:

* database `yodeck`
* schema `raw`
* schema `staging`
* schema `marts`

Create a dedicated PostgreSQL user and grant the required permissions.

---

## 4. Configure dlt

Create:

```
ingestion/.dlt/secrets.toml
```

Example:

```toml
[destination.postgres.credentials]

host = "localhost"
port = 5432
database = "yodeck"

username = "your_user"
password = "your_password"
```

---

## 5. Configure AWS

Configure AWS CLI with credentials that have read access to the S3 bucket:

```bash
aws configure
```

---

## 6. Execute the ingestion pipeline

Load every snapshot:

```bash
cd ingestion

python load_s3_csvs.py --mode all
```

Load a date range:

```bash
python load_s3_csvs.py \
    --mode range \
    --start-date 2026-06-09 \
    --end-date 2026-06-12
```

Load only newly discovered snapshots:

```bash
python load_s3_csvs.py --mode new
```

---

## 7. Execute dbt

```bash
cd dbt

dbt build
```

---

# Assignment Notes

The supplied invoice snapshots intentionally contain invalid dates.

The ingestion framework correctly rejects these files during datatype validation, preventing invalid data from entering the raw layer. Consequently, the invoice staging model is not materialized, demonstrating fail-fast validation at the ingestion layer.

Ingestion run logs are included in this repository under:

`logs/ingestion.log`

The log captures the execution of the ingestion framework and provides useful operational information, including:

* Number of snapshot files discovered and selected for ingestion.
* Number of successfully ingested and failed files.
* Detection of duplicate snapshot files.
* Detection of incomplete business snapshots.
* Schema evolution events, including the affected file and the columns that were added or removed.
* Validation failures (e.g. datatype, primary key or filename validation).
* Per-file processing status and final ingestion summary.

These logs are intended to demonstrate the pipeline's monitoring and observability capabilities and provide reviewers with complete execution traces without needing to rerun the pipeline.

Repository log:
https://github.com/Sofiaging/yodeck-data-engineering-assignment/blob/main/logs/ingestion.log