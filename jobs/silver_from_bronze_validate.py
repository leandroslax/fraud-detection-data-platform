#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

BUCKET = "fraud-datalake-dev-use1-139961319000"
BRONZE_PREFIX = "bronze/source=stream/entity=fraud_events/"
SILVER_WAREHOUSE = f"s3://{BUCKET}/silver_iceberg/"
DB = "fraud_lake"

SILVER_TABLE = "silver_fraud_events_iceberg"
QUAR_TABLE = "silver_fraud_events_quarantine"
AUDIT_TABLE = "silver_fraud_events_audit"

VALID_CURRENCIES = ["BRL", "USD", "EUR", "GBP", "ARS"]
VALID_COUNTRIES = ["BR", "US", "UK", "AR"]
VALID_CHANNELS = ["WEB", "POS", "ECOMMERCE", "MOBILE", "API"]
VALID_STATUS = ["APPROVED", "DECLINED"]

event_schema = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("transaction_id", T.StringType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("merchant_id", T.StringType()),
    T.StructField("amount", T.DoubleType()),
    T.StructField("currency", T.StringType()),
    T.StructField("payment_method", T.StringType()),
    T.StructField("channel", T.StringType()),
    T.StructField("country", T.StringType()),
    T.StructField("region", T.StringType()),
    T.StructField("city", T.StringType()),
    T.StructField("device_id", T.StringType()),
    T.StructField("ip_address", T.StringType()),
    T.StructField("risk_score", T.DoubleType()),
    T.StructField("is_international", T.IntegerType()),
    T.StructField("status", T.StringType()),
    T.StructField("event_ts", T.StringType()),
    T.StructField("event_year", T.StringType()),
    T.StructField("event_month", T.StringType()),
    T.StructField("event_day", T.StringType()),
    T.StructField("event_time", T.StringType()),
])

def ensure_tables(spark: SparkSession):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{DB}")

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{DB}.{SILVER_TABLE} (
      event_id string,
      transaction_id string,
      user_id string,
      merchant_id string,
      amount double,
      currency string,
      payment_method string,
      channel string,
      country string,
      region string,
      city string,
      device_id string,
      ip_address string,
      risk_score double,
      is_international int,
      status string,
      event_ts timestamp,
      ingestion_ts timestamp,
      source_file string,
      dq_status string,
      dq_errors array<string>,
      ds_day date
    )
    USING iceberg
    PARTITIONED BY (ds_day)
    LOCATION '{SILVER_WAREHOUSE}{SILVER_TABLE}/'
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{DB}.{QUAR_TABLE} (
      ingestion_ts timestamp,
      source_file string,
      raw_line string,
      dq_status string,
      dq_errors array<string>,
      dq_hard_errors array<string>,
      event_id string,
      transaction_id string,
      user_id string,
      event_ts string,
      ds_day date
    )
    USING iceberg
    PARTITIONED BY (ds_day)
    LOCATION '{SILVER_WAREHOUSE}{QUAR_TABLE}/'
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{DB}.{AUDIT_TABLE} (
      ds_day date,
      dq_status string,
      records bigint,
      audit_ts timestamp
    )
    USING iceberg
    PARTITIONED BY (ds_day)
    LOCATION '{SILVER_WAREHOUSE}{AUDIT_TABLE}/'
    """)

def fix_iceberg_deprecated_props(spark: SparkSession):
    """
    Remove 'write.object-storage.path' (deprecated) e opcionalmente define 'write.data.path'.
    Isso evita o erro: Property 'write.object-storage.path' has been deprecated...
    """
    targets = [
        (SILVER_TABLE, f"{SILVER_WAREHOUSE}{SILVER_TABLE}/data"),
        (QUAR_TABLE,   f"{SILVER_WAREHOUSE}{QUAR_TABLE}/data"),
        (AUDIT_TABLE,  f"{SILVER_WAREHOUSE}{AUDIT_TABLE}/data"),
    ]

    for table, data_path in targets:
        full = f"glue_catalog.{DB}.{table}"

        # Remove a propriedade antiga (se existir)
        try:
            spark.sql(f"ALTER TABLE {full} UNSET TBLPROPERTIES ('write.object-storage.path')")
        except Exception:
            pass

        # Define a propriedade nova (opcional, mas recomendado)
        try:
            spark.sql(f"ALTER TABLE {full} SET TBLPROPERTIES ('write.data.path'='{data_path}')")
        except Exception:
            pass

def main():
    spark = SparkSession.builder.appName("silver_from_bronze_validate").getOrCreate()

    ensure_tables(spark)
    fix_iceberg_deprecated_props(spark)

    bronze_path = f"s3://{BUCKET}/{BRONZE_PREFIX}"
    # Para teste rápido (um dia), descomente:
    # bronze_path = f"s3://{BUCKET}/{BRONZE_PREFIX}year=2025/month=12/day=27/"

    # 1) Lê .gz como texto (Spark descompacta gzip automaticamente)
    raw = (
        spark.read.format("text")
        .load(bronze_path)
        .withColumnRenamed("value", "raw_line")
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
    )

    # 2) Parse JSON
    parsed = raw.withColumn("j", F.from_json(F.col("raw_line"), event_schema))

    df = (
        parsed.withColumn("is_malformed_json", F.col("j").isNull())
              .select("raw_line", "ingestion_ts", "source_file", "is_malformed_json", F.col("j.*"))
    )

    # 3) Normalizações + casts
    df = (
        df.withColumn("currency", F.upper(F.trim(F.col("currency"))))
          .withColumn("country", F.upper(F.trim(F.col("country"))))
          .withColumn("channel", F.upper(F.trim(F.col("channel"))))
          .withColumn("status", F.upper(F.trim(F.col("status"))))
          .withColumn("event_ts_ts", F.to_timestamp(F.col("event_ts")))
          .withColumn("ds_day", F.to_date(F.col("event_ts_ts")))
    )

    # 4) Regras DQ (sem array_remove; remove null via expr filter)
    errors_raw = F.array(
        F.when(F.col("raw_line").isNotNull() & F.col("is_malformed_json"), F.lit("MALFORMED_JSON")),
        F.when(F.col("event_id").isNull() | (F.length(F.trim(F.col("event_id"))) == 0), F.lit("MISSING_EVENT_ID")),
        F.when(F.col("transaction_id").isNull() | (F.length(F.trim(F.col("transaction_id"))) == 0), F.lit("MISSING_TRANSACTION_ID")),
        F.when(F.col("user_id").isNull() | (F.length(F.trim(F.col("user_id"))) == 0), F.lit("MISSING_USER_ID")),
        F.when(F.col("event_ts_ts").isNull(), F.lit("INVALID_EVENT_TS")),
        F.when(F.col("amount").isNull(), F.lit("MISSING_AMOUNT")),
        F.when(F.col("amount").isNotNull() & (F.col("amount") <= 0), F.lit("AMOUNT_NON_POSITIVE")),
        F.when(~F.col("currency").isin(*VALID_CURRENCIES), F.lit("INVALID_CURRENCY")),
        F.when(~F.col("country").isin(*VALID_COUNTRIES), F.lit("INVALID_COUNTRY")),
        F.when(F.col("channel").isNull() | (~F.col("channel").isin(*VALID_CHANNELS)), F.lit("INVALID_CHANNEL")),
        F.when(F.col("status").isNull() | (~F.col("status").isin(*VALID_STATUS)), F.lit("INVALID_STATUS")),
        F.when(F.col("risk_score").isNotNull() & ((F.col("risk_score") < 0) | (F.col("risk_score") > 1)), F.lit("RISK_SCORE_OUT_OF_RANGE")),
        F.when(
            F.col("event_ts_ts").isNotNull() &
            (F.col("event_ts_ts") > (F.current_timestamp() + F.expr("INTERVAL 5 MINUTES"))),
            F.lit("EVENT_TS_IN_FUTURE")
        )
    )

    df = df.withColumn("errors_raw", errors_raw)
    df = df.withColumn("dq_errors", F.expr("filter(errors_raw, x -> x is not null)")).drop("errors_raw")

    # 5) Hard errors => BAD
    hard_list = [
        "MALFORMED_JSON",
        "MISSING_EVENT_ID", "MISSING_TRANSACTION_ID", "MISSING_USER_ID",
        "INVALID_EVENT_TS", "MISSING_AMOUNT",
        "AMOUNT_NON_POSITIVE", "INVALID_CURRENCY", "INVALID_COUNTRY",
        "INVALID_CHANNEL", "INVALID_STATUS", "EVENT_TS_IN_FUTURE"
    ]

    df = df.withColumn(
        "dq_hard_errors",
        F.array_intersect(F.col("dq_errors"), F.array(*[F.lit(x) for x in hard_list]))
    )

    df = df.withColumn(
        "dq_status",
        F.when(F.size(F.col("dq_hard_errors")) > 0, F.lit("BAD"))
         .when(F.size(F.col("dq_errors")) > 0, F.lit("WARN"))
         .otherwise(F.lit("GOOD"))
    )

    good = df.filter(F.col("dq_status").isin("GOOD", "WARN"))
    bad = df.filter(F.col("dq_status") == "BAD")

    # 6) BAD -> quarantine
    (
        bad.select(
            "ingestion_ts", "source_file", "raw_line",
            "dq_status", "dq_errors", "dq_hard_errors",
            "event_id", "transaction_id", "user_id", "event_ts", "ds_day"
        )
        .writeTo(f"glue_catalog.{DB}.{QUAR_TABLE}")
        .append()
    )

    # 7) GOOD/WARN -> silver
    (
        good.select(
            "event_id", "transaction_id", "user_id", "merchant_id",
            "amount", "currency", "payment_method", "channel",
            "country", "region", "city", "device_id", "ip_address",
            "risk_score", "is_international", "status",
            F.col("event_ts_ts").alias("event_ts"),
            "ingestion_ts", "source_file", "dq_status", "dq_errors",
            "ds_day"
        )
        .writeTo(f"glue_catalog.{DB}.{SILVER_TABLE}")
        .append()
    )

    # 8) Auditoria
    audit = (
        df.groupBy("ds_day", "dq_status")
          .agg(F.count("*").alias("records"))
          .withColumn("audit_ts", F.current_timestamp())
    )

    (
        audit.writeTo(f"glue_catalog.{DB}.{AUDIT_TABLE}")
        .append()
    )

    spark.stop()

if __name__ == "__main__":
    main()