# Getting Started

This guide explains how to run the Fraud Detection Data Platform end-to-end.

---

# 1. Prerequisites

Make sure you have:

- AWS CLI configured (default profile)
- Access to:
  - Amazon Kinesis
  - Amazon S3
  - EMR Serverless
  - MWAA (Airflow)
  - AWS Glue
  - Amazon Athena
  - Amazon QuickSight
- Python 3.10+
- boto3 installed

Check AWS CLI:

```bash
aws sts get-caller-identity
```

---

# 2. Verify Required AWS Resources

Ensure the following exist:

## Kinesis Stream
- fraud-events-stream

## S3 Buckets
- s3://fraud-datalake-dev-use1-139961319000
- s3://fraud-mwaa-artifacts-dev-use1-139961319000
- s3://aws-athena-query-results-us-east-1-139961319000

## EMR Serverless Application
Confirm application ID:

```bash
aws emr-serverless list-applications
```

---

# 3. Run the Fraud Event Producer

Navigate to:

```
producer/
```

Run:

```bash
python3 kinesis-producer.py
```

This will generate simulated fraud events and push them into:

Amazon Kinesis → fraud-events-stream

---

# 4. Validate Bronze Layer

After producer runs, verify Bronze data in S3:

```bash
aws s3 ls s3://fraud-datalake-dev-use1-139961319000/bronze/ --recursive | head -n 20
```

You should see partitions like:

```
year=2026/month=02/day=27/
```

---

# 5. Run the Airflow DAG

Open MWAA UI.

Trigger DAG:

```
fraud_end_to_end_bronze_silver_gold_emr_serverless
```

Pipeline steps:

1. Wait for Bronze partition
2. Run EMR job (Bronze → Silver)
3. Run EMR job (Silver → Gold)
4. Validate output

---

# 6. Validate Silver Layer

```bash
aws s3 ls s3://fraud-datalake-dev-use1-139961319000/silver/ --recursive | head -n 20
```

---

# 7. Validate Gold (Iceberg)

In Athena, run:

```sql
SELECT *
FROM fraud_lake.fraud_events_geo_daily
ORDER BY ds_day DESC
LIMIT 10;
```

If data appears, the pipeline executed successfully.

---

# 8. View Dashboard

Open Amazon QuickSight.

Load dataset connected to Athena (Iceberg Gold tables).

You should see:
- Fraud events by country
- Risk score trends
- Approved vs Declined transactions
- Daily KPI metrics

---

# 9. Common Issues

If Bronze partition not detected:
- Check producer running
- Confirm correct S3 prefix

If EMR job fails:
- Check CloudWatch logs
- Validate IAM role permissions

If Athena query fails:
- Confirm Iceberg table exists in Glue Catalog
- Validate S3 path permissions

---

# Success Criteria

The pipeline is working when:

- Bronze files exist in S3
- Silver files are generated
- Gold Iceberg tables return data in Athena
- Dashboard shows updated metrics
