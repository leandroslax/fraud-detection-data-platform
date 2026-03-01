# Fraud Data Platform – AWS Lakehouse Architecture

## 1. Overview

This project implements a **serverless-first Lakehouse architecture on AWS** for fraud event ingestion, transformation and analytics.

The solution follows the **Medallion Architecture pattern (Bronze → Silver → Gold)** and leverages **Apache Iceberg** for the analytics layer.

Processing is executed using **EMR Serverless (Spark)** and orchestrated by **MWAA (Managed Apache Airflow)**.

---

# 2. End-to-End High-Level Architecture

```text
                               ┌──────────────────────────────────┐
                               │        Fraud Event Producer       │
                               │        (Python / boto3)           │
                               └───────────────────┬───────────────┘
                                                   │
                                                   ▼
                                        ┌───────────────────┐
                                        │   Amazon Kinesis   │
                                        │ fraud-events-stream│
                                        └─────────┬─────────┘
                                                  │
                                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                             Amazon S3 - Data Lake                           │
│                   s3://fraud-datalake-dev-use1-139961319000                 │
│                                                                             │
│  Bronze (raw)     Silver (clean)     Gold (BI - Iceberg)                    │
│  ─────────────     ─────────────     ───────────────────                    │
│  bronze/...        silver/...        gold_iceberg/...                       │
│  year=YYYY/...     year=YYYY/...     (Glue Catalog + Iceberg)               │
└─────────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
                        ┌──────────────────────────┐
                        │     EMR Serverless       │
                        │   Spark (PySpark jobs)   │
                        └────────────┬─────────────┘
                                     │
                                     ▼
                        ┌──────────────────────────┐
                        │   AWS Glue Data Catalog  │
                        └────────────┬─────────────┘
                                     │
                                     ▼
                        ┌──────────────────────────┐
                        │      Amazon Athena       │
                        └────────────┬─────────────┘
                                     │
                                     ▼
                        ┌──────────────────────────┐
                        │    Amazon QuickSight     │
                        └──────────────────────────┘
```

---

# 3. Storage Architecture

## 3.1 Primary Data Lake Bucket

```
s3://fraud-datalake-dev-use1-139961319000
```

This bucket stores all Medallion layers and Iceberg warehouse data.

---

## 3.2 Bronze Layer (Raw)

Purpose:
- Store events exactly as received from Kinesis
- Allow replay/reprocessing
- Preserve raw audit history

Partition pattern:

```
bronze/source=stream/entity=fraud_events/year=YYYY/month=MM/day=DD/
```

Characteristics:
- Minimal transformation
- Flexible schema
- Append-only ingestion

---

## 3.3 Silver Layer (Clean / Curated)

Purpose:
- Standardize schema
- Enforce data quality
- Enrich records

Partition pattern:

```
silver/entity=fraud_events/year=YYYY/month=MM/day=DD/
```

Typical transformations:
- Timestamp normalization to UTC
- Data type casting
- Deduplication by `event_id`
- Country and region enrichment
- Risk bucket classification

---

## 3.4 Gold Layer (Analytics – Lakehouse)

The Gold layer uses **Apache Iceberg tables** stored in S3 and registered in AWS Glue.

Structure:

```
gold_iceberg/<database>/<table>/data/
gold_iceberg/<database>/<table>/metadata/
```

Registered in:
- AWS Glue Data Catalog

Queried by:
- Amazon Athena
- Amazon QuickSight

Example tables:
- fraud_events_kpis_daily
- fraud_events_geo_daily
- fraud_events_payment_method_daily

---

# 4. Supporting Buckets

## 4.1 MWAA (Airflow Artifacts)

```
s3://fraud-mwaa-artifacts-dev-use1-139961319000/
```

Stores:
- DAGs
- Airflow plugins
- Workflow artifacts

---

## 4.2 Athena Query Results

```
s3://aws-athena-query-results-us-east-1-139961319000/
```

Stores:
- Query outputs
- Temporary SQL results

---

## 4.3 Databricks Managed Storage

```
s3://databricks-storage-7474655580663727/
```

Used internally by the Databricks workspace.

Note:
This is not the official Data Lake bucket for this project.

---

# 5. Processing Layer

## EMR Serverless (Spark)

Responsibilities:
- Bronze → Silver transformations
- Silver → Gold aggregations
- Iceberg table writes

Advantages:
- No cluster management
- Auto-scaling
- Pay-per-use
- Native Spark support

---

# 6. Orchestration

## MWAA (Managed Apache Airflow)

Pipeline control:
1. Wait for Bronze partition
2. Trigger Silver transformation job
3. Validate Silver output
4. Trigger Gold aggregation job
5. Validate Gold KPIs

Benefits:
- Fully managed orchestration
- Decoupled execution
- Retry and monitoring support

---

# 7. Lakehouse Design Decisions

## Why Apache Iceberg?

- Schema evolution support
- Partition management
- Metadata tracking
- Athena compatibility
- Open table format
- Time-travel capability (if needed)

## Why EMR Serverless?

- Serverless-first architecture
- Reduced operational overhead
- Native Spark compatibility
- Seamless integration with S3 and Glue

---

# 8. Architectural Principles

- Streaming ingestion
- Medallion Architecture
- Lakehouse over S3
- Serverless-first AWS stack
- Fully decoupled storage and compute
- Scalable and cost-efficient design

---

# 9. Future Improvements

- Iceberg table compaction strategy
- Data quality framework integration
- Lake Formation governance
- CI/CD for Spark jobs and DAGs
- Automated testing layer
- Observability and SLA metrics
